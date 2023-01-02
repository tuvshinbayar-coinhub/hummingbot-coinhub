import logging
import math
from copy import copy
from decimal import Decimal
from random import randrange, uniform
from typing import Iterator, List, Tuple

import pandas as pd
from redblacktree import rbtree

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange_base import ExchangeBase, PriceType
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_query_result import ClientOrderBookQueryResult
from hummingbot.core.data_type.order_book_row import ClientOrderBookRow
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    OrderBookEvent,
    OrderBookTradeEvent,
    OrderFilledEvent,
    OrderType,
    SellOrderCreatedEvent,
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

s_logger = None

def apply_spread(side: TradeType, price: Decimal, spread: Decimal):
    mult = 1 + (spread if side == TradeType.SELL else -spread)
    return price * mult

def remove_spread(side: TradeType, price: Decimal, spread: Decimal):
    mult = 1 + (spread if side == TradeType.SELL else -spread)
    return price / mult

class PricePoint:
    def __init__(self, price_point: Decimal, weighted_price: Decimal = None) -> None:
        self.price_point = price_point
        self.weighted_price = weighted_price

    def set_weighted_price(self, price: Decimal) -> None:
        self.weighted_price = price

    def __repr__(self):
        return f'PricePoint("{self.price_point}")'

    @property
    def price(self) -> Decimal:
        if self.weighted_price is not None:
            return self.weighted_price
        return self.price_point

class Base:
    def __init__(self, asks: rbtree, bids: rbtree, **kwargs) -> None:
        self.book = {
            TradeType.BUY: bids if not bids else rbtree(),
            TradeType.SELL: asks if not asks else rbtree()
        }
        self._volume_bids_base = kwargs["volume_bids_base"]
        self._volume_asks_base = kwargs["volume_asks_base"]
        self._volume_bids_quote = kwargs["volume_bids_quote"]
        self._volume_asks_quote = kwargs["volume_asks_quote"]
    
    @property
    def buy(self) -> rbtree:
        return self.book[TradeType.BUY]

    @property
    def sell(self) -> rbtree:
        return self.book[TradeType.SELL]

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    @property
    def volume_bids_base(self) -> Decimal:
        if self._volume_bids_base is None:
            return sum([bid.amount for bid in self.buy])
        return self._volume_bids_base
    
    @property
    def volume_bids_quote(self) -> Decimal:
        if self._volume_bids_quote is None:
            return sum([bid.amount * bid.price for bid in self.buy])
        return self._volume_bids_quote
    
    @property
    def volume_asks_base(self) -> Decimal:
        if self._volume_asks_base is None:
            return sum([ask.amount for ask in self.sell])
        return self._volume_asks_base

    @property
    def volume_asks_quote(self) -> Decimal:
        if self._volume_asks_quote is None:
            return sum([ask.amount * ask.price for ask in self.sell])
        return self._volume_asks_quote

    def better(self, side: TradeType, a: Decimal, b: Decimal) -> bool:
        if side == TradeType.BUY:
            return a > b
        else:
            return a < b

    def spread(self, bids_spread, asks_spread):
        asks_spread = 1 + asks_spread
        bids_spread = 1 - bids_spread
        bids = rbtree()
        asks = rbtree()
        volume_bids_quote = Decimal("0.0")
        volume_asks_quote = Decimal("0.0")
        for order_price, order_volume in self.buy:
            price = order_price * bids_spread
            bids[price] = order_volume
            volume_bids_quote += price * order_volume
        for order_price, order_volume in self.sell:
            price = order_price * asks_spread
            asks[price] = order_volume
            volume_asks_quote += price * order_volume
        return self.__class__(
            asks=asks,
            bids=bids,
            volume_bids_quote=volume_bids_quote,
            volume_bids_base=self.volume_bids_base,
            volume_asks_quote=volume_asks_quote,
            volume_asks_base=self.volume_asks_base
            )

    def group_by_level(self, side: TradeType, price_points: List[PricePoint]) -> List[dict]:
        if len(price_points) == 0:
            return []
        result = []
        level_index = 0
        init_level = lambda price_point: {
            "price": price_point,
            "orders": []
        }
        result[0] = init_level(price_points[0].price_point)
        for order_price, order_volume in self.book[side]:
            while level_index < len(price_points) and self.better(side, price_points[level_index].price_point, order_price):
                level_index += 1
                try:
                    price_point = price_points[level_index].price_point
                    result[level_index] = init_level(price_point)
                except:
                    break
            if level_index >= len(price_points):
                break
            price_point = price_points[level_index].price_point
            try:
                result[level_index]
            except IndexError:
                result[level_index] = init_level(price_point)
            result[level_index]["orders"].append(order_volume)
        return result
            

class Aggregated(Base):
    def to_ob(self) -> OrderBook:
        asks = rbtree()
        bids = rbtree()
        volume_bids_base = 0
        volume_asks_base = 0
        volume_bids_quote = 0
        volume_asks_quote = 0

        for idx, (price, data) in enumerate(self.sell):
            order = ClientOrderBookRow(price=Decimal(price), amount=Decimal(data["volume"]), update_id=idx)
            asks[order.price] = order.amount
            volume_asks_base += order.amount
            volume_asks_quote += order.amount * order.price
        
        for idx, (price, data) in enumerate(self.buy):
            order = ClientOrderBookRow(price=Decimal(price), amount=Decimal(data["volume"]), update_id=idx)
            bids[order.price] = order.amount
            volume_bids_base += order.amount
            volume_bids_quote += order.amount * order.price
        
        return Orderbook(asks=asks, bids=bids)

class Orderbook(Base):
    def new_price(self, price: Decimal, volume: Decimal):
        return {
            "volume": volume,
            "high_price": price,
            "low_price": price,
            "volume_price": volume
        }

    def group_by_price_points(self, side: TradeType, price_points: List[PricePoint], minimum_volume) -> Tuple[rbtree, Decimal, Decimal]:
        price_points_copy = copy(price_points)
        tree = rbtree()
        volume_base = Decimal("0.0")
        volume_quote = Decimal("0.0")
        for order_price, order_volume in self.book[side]:
            while len(price_points_copy) > 0 and self.better(side, price_points_copy[0].price_point, order_price):
                price_point = price_points_copy[0].price_point
                # Create an order with minimum volume if there is no order in range
                if not price_point in tree or (price_point in tree and tree[price_point].get("volume", 0) == 0):
                    tree[price_point] = self.new_price(price_point, minimum_volume)
                    volume_base += minimum_volume
                    volume_quote += minimum_volume * price_point
                price_points_copy.pop(0)
            if len(price_points_copy) == 0:
                break
            price_point = price_points_copy[0].price_point
            tree[price_point] = tree[price_point] if tree[price_point] else self.new_price(order_price, Decimal("0.0"))
            volume_base += order_volume
            volume_quote += order_volume * order_price
            tree[price_point]["volume"] += order_volume
            if order_price > tree[price_point]["high_price"]:
                tree[price_point]["high_price"] = order_price
            if order_price < tree[price_point]["low_price"]:
                tree[price_point]["low_price"] = order_price
            tree[price_point]["volume_price"] += order_price * order_volume
        return (tree, volume_base, volume_quote)

    def aggregate_side(self, side: TradeType, price_points: List[PricePoint], minimum_volume = 0.1) -> Tuple[rbtree, Decimal, Decimal]:
        if price_points is None:
            return (None, None, None)
        tree, volume_base, volume_quote = self.group_by_price_points(side, price_points, minimum_volume)
        # Add remaining price points in case there is no more orders in the input
        for pp in price_points:
            if pp.price_point in tree:
                continue
            tree[pp.price] = self.new_price(pp.price_point, minimum_volume)
            volume_base += minimum_volume
            volume_quote += minimum_volume * pp.price_point
        wtree = rbtree()
        for price_point, data in tree:
            wtree[price_point] = {
                "volume": data["volume"],
                "high_price": data["high_price"],
                "low_price": data["low_price"],
                "weighted_price": data["volume_price"] / data["volume"],
            }
        final_tree = rbtree()
        for price_point, data in wtree:
            final_tree[data["weighted_price"]] = data
        return (final_tree, volume_base, volume_quote)

    def aggregate(self, price_points_buy, price_points_sell, min_amount) -> Aggregated:
        bids_obs, _vol_bids_base, _vol_bids_quote = self.aggregate_side(TradeType.BUY, price_points_buy, min_amount)
        asks_obs, _vol_asks_base, _vol_asks_quote = self.aggregate_side(TradeType.SELL, price_points_sell, min_amount)
        return Aggregated(
            asks=asks_obs,
            bids=bids_obs,
            volume_bids_quote=_vol_bids_quote,
            volume_bids_base=_vol_bids_base,
            volume_asks_quote=_vol_asks_quote,
            volume_asks_base=_vol_asks_base
        )
    
    def adjust_volume(self, limit_bids_base, limit_asks_base, limit_bids_quote = None, limit_asks_quote = None):
        if limit_bids_base and (limit_bids_base < self.volume_bids_base):
            volume_bids_base = Decimal("0.0")
            volume_bids_quote = Decimal("0.0")
            bids = rbtree()
            do_break = False
            for order_price, order_volume in self.buy:
                amount = (limit_bids_base * order_volume / self.volume_bids_base)
                amount_quote = amount * order_price
                if limit_bids_quote and (volume_bids_quote + amount_quote > limit_bids_quote):
                    amount_quote = limit_bids_quote -  volume_bids_quote
                    amount = amount_quote / order_price
                    self.logger().warn(f"Bids volume throttled to {amount} because of quote currency")
                    do_break = True
                bids[order_price] = amount
                volume_bids_base += amount
                volume_bids_quote += amount_quote
                if do_break:
                    break
        else:
            volume_bids_base = self.volume_bids_base
            volume_bids_quote = self.volume_bids_quote
            bids = self.buy
        
        if limit_asks_base and (limit_asks_base < self.volume_asks_base):
            volume_asks_base = Decimal("0.0")
            volume_asks_quote = Decimal("0.0")
            asks = rbtree()
            do_break = False
            for order_price, order_volume in self.sell:
                amount = (limit_asks_base * order_volume / self.volume_asks_base)
                amount_quote = amount * order_price
                if limit_asks_quote and (volume_asks_quote + amount_quote > limit_asks_quote):
                    amount_quote = limit_asks_quote -  volume_asks_quote
                    amount = amount_quote / order_price
                    self.logger().warn(f"Asks volume throttled to {amount} because of quote currency")
                    do_break = True
                asks[order_price] = amount
                volume_asks_base += amount
                volume_asks_quote += amount_quote
                if do_break:
                    break
        else:
            volume_asks_base = self.volume_asks_base
            volume_asks_quote = self.volume_asks_quote
            asks = self.sell
        return Orderbook(
                asks=asks,
                bids=bids,
                volume_bids_quote=volume_bids_quote,
                volume_bids_base=volume_bids_base,
                volume_asks_quote=volume_asks_quote,
                volume_asks_base=volume_asks_base
            )
        

class Orderback(ScriptStrategyBase):
    """
    This strategy creates random trades on a market with random amounts following the price of one or several sources.
    It is commonly used to create candles on a market with low activity.
    Before running this example, make sure:
      - Run `config rate_oracle_source coinbase`
      - Set Maker market
      - Set Taker market
      - Set conversion_rate
    """

    target_market: str = "coinhub_sandbox"
    target_market_pair: str = "BTC-MNT"
    target_base_asset, target_quote_asset = split_hb_trading_pair(target_market_pair)
    source_market: str = "binance_paper_trade"
    source_market_pair: str = "BTC-USDT"
    source_base_asset, source_quote_asset = split_hb_trading_pair(source_market_pair)

    conversion_pair: str = f"{source_quote_asset}-{target_quote_asset}"
    markets = {target_market: {target_market_pair}, source_market: {source_market_pair}}

    _last_timestamp = 0
    status_report_interval = 900
    should_report_warnings = 0

    _all_markets_ready = False
    _conversions_ready = False

    order_refresh_time = 15
    create_timestamp = 0

    # Orderback trading configs
    # Buy side spread percentage
    _spread_bids = 1
    # Sell side spread percentage
    _spread_asks = 1
    # Minimum price difference between levels
    levels_price_step = Decimal("0.1")
    # Number of orders for each side
    levels_count = 25
    # constant, linear, exp
    levels_price_func = "constant"

    subscribed_to_order_book_trade_event: bool = False

    limit_bids_base = Decimal("10.0")
    limit_asks_base = Decimal("10.0")
    limit_by_source_balance = True

    @property
    def target(self) -> ExchangeBase:
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.target_market]

    @property
    def source(self) -> ExchangeBase:
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.source_market]

    def on_tick(self):
        """
        Runs every tick_size seconds, this is the main operation of the strategy.
        - Create proposal (a list of order candidates)
        - Check the account balance and adjust the proposal accordingly (lower order amount if needed)
        - Lastly, execute the proposal on the exchange
        """
        current_tick = (self.current_timestamp // self.status_report_interval)
        last_tick = (self._last_timestamp // self.status_report_interval)
        self.should_report_warnings = (current_tick > last_tick)

        if not self.is_ready:
            return
        if not self.subscribed_to_order_book_trade_event:
            # Set pandas resample rule for a timeframe
            self.subscribe_to_order_book_trade_event()
        if self.create_timestamp <= self.current_timestamp:
            self.cancel_all_orders()
            safe_ensure_future(self.main())
            self.create_timestamp = self.order_refresh_time + self.current_timestamp
    
    def is_ready(self) -> bool:
        if not self._all_markets_ready or not self._rate_oracle_ready:
            self._all_markets_ready = all([market.ready for market in self.active_markets])
            if not self._all_markets_ready:
                # Markets not ready yet. Don't do anything.
                if self.should_report_warnings:
                    self.logger().warning("Markets are not ready. No market making trades are permitted.")
                return False
            else:
                # Markets are ready, ok to proceed.
                self.logger().info("Markets are ready.")

        if not self._conversions_ready:
            for market_pair in self._market_pairs.values():
                _, _, quote_rate, _, _, base_rate, _, _, _ = self.get_conversion_rates(market_pair)
                if not quote_rate or not base_rate:
                    if self.should_report_warnings:
                        self.logger().warning("Conversion rates are not ready. No market making trades are permitted.")
                    return False

            # Conversion rates are ready, ok to proceed.
            self._conversions_ready = True
            self.logger().info("Conversion rates are ready. Trading started.")

        if self.should_report_warnings:
            # Check if all markets are still connected or not. If not, log a warning.
            if not all([market.network_status is NetworkStatus.CONNECTED for market in self.active_markets]):
                self.logger().warning("WARNING: Some markets are not connected or are down at the moment. Market "
                                      "making may be dangerous when markets or networks are unstable.")
        return True

    async def main(self):
        desired_ob, price_levels = await self.call()

    def call(self) -> Tuple(Orderbook, dict):
        top_ask_price = self.source.get_price_by_type(self.source_market_pair, PriceType.BestAsk)
        top_bid_price = self.source.get_price_by_type(self.source_market_pair, PriceType.BestBid)

        price_points_asks = self.price_points(False, top_ask_price)
        price_points_bids = self.price_points(True, top_bid_price)

        orderbook_aggregator = Orderbook(asks=self.source.order_book_ask_entries(self.source_market_pair), bids=self.source.order_book_bid_entries(self.source_market_pair))
        ob_agg = orderbook_aggregator.aggregate(price_points_sell=price_points_asks, price_points_buy=price_points_bids, min_amount=self.target.trading_rules[self.target_market_pair].min_order_size)
        ob = ob_agg.to_ob()
        limit_asks_quote = self.source.get_available_balance(self.source_quote_asset)
        limit_bids_quote = self.target.get_balance(self.target_quote_asset)
        source_base_free = self.source.get_available_balance(self.source_base_asset)
        target_base_total = self.target.get_balance(self.target_base_asset)
        limit_bids_base_applied = self.limit_bids_base
        limit_asks_base_applied = self.limit_asks_base
        if source_base_free < limit_bids_base_applied and self.limit_by_source_balance:
            limit_bids_base_applied = source_base_free
            self.logger().warn(f"{self.source_base_asset} balance on {self.source.name} is {source_base_free} lower than the limit set to {self.limit_bids_base}")
        if target_base_total < limit_asks_base_applied and self.limit_by_source_balance:
            limit_asks_base_applied = target_base_total
            self.logger().warn(f"{self.target_base_asset} balance on {self.target.name} is {target_base_total} lower than the limit set to {self.limit_asks_base}")
        ob_adjusted = ob.adjust_volume(
            limit_bids_base_applied,
            limit_asks_base_applied,
            limit_bids_quote,
            limit_asks_quote
        )
        ob_spread = ob_adjusted.spread(self._spread_bids, self._spread_asks)
        price_points_asks: List[PricePoint] = [PricePoint(apply_spread(TradeType.SELL, pp.price_point, self._spread_asks)) for pp in price_points_asks]
        price_points_bids: List[PricePoint] = [PricePoint(apply_spread(TradeType.BUY, pp.price_point, self._spread_bids)) for pp in price_points_bids]
        return (ob_spread, { "asks": price_points_asks, "bids": price_points_bids })

    def price_points(self, side: TradeType, price_start: Decimal) -> List[PricePoint]:
        points = []
        last_price = price_start
        for i in range(self.levels_count):
            price_delta = self.price_step(i)
            price = last_price + price_delta
            if side == TradeType.BUY:
                price = last_price - price_delta
            points.append(PricePoint(price_point=price))
            last_price = price
        return points
    
    def price_step(self, level_i: int):
        price_functions = {
            "constant": self.levels_price_step,
            "linear": (Decimal(f"{level_i}") + 1) * self.levels_price_step,
            "exp": Decimal(f"{math.exp(level_i)}") * self.levels_price_step
        }
        return price_functions.get(self.levels_price_func)

    def cancel_all_orders(self):
        for order in self.get_active_orders(connector_name=self.target_market):
            self.cancel(self.exchange, order.trading_pair, order.client_order_id)
    
    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Exchange", "Market", "Side", "Price", "Amount", "Spread Mid", "Spread Cancel", "Age"]
        data = []
        mid_price = self.connectors[self.maker_exchange].get_mid_price(self.maker_pair)
        taker_buy_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair, True, self.order_amount)
        taker_sell_result = self.connectors[self.taker_exchange].get_price_for_volume(self.taker_pair, False, self.order_amount)
        buy_cancel_threshold = taker_sell_result.result_price * Decimal(1 - self.min_spread_bps / 10000)
        sell_cancel_threshold = taker_buy_result.result_price * Decimal(1 + self.min_spread_bps / 10000)
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                spread_mid_bps = (mid_price - order.price) / mid_price * 10000 if order.is_buy else (order.price - mid_price) / mid_price * 10000
                spread_cancel_bps = (buy_cancel_threshold - order.price) / buy_cancel_threshold * 10000 if order.is_buy else (order.price - sell_cancel_threshold) / sell_cancel_threshold * 10000
                data.append([
                    self.maker_exchange,
                    order.trading_pair,
                    "buy" if order.is_buy else "sell",
                    float(order.price),
                    float(order.quantity),
                    int(spread_mid_bps),
                    int(spread_cancel_bps),
                    age_txt
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", "Side"], inplace=True)
        return df

    def subscribe_to_order_book_trade_event(self):
        """
        Subscribe to raw trade event.
        """
        # self.order_book_trade_event = SourceInfoEventForwarder(self._process_public_trade)
        # for order_book in self.maker.order_books.values():
        #     order_book.add_listener(OrderBookEvent.TradeEvent, self.order_book_trade_event)
        self.subscribed_to_order_book_trade_event = True