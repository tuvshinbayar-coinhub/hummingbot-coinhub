import math
from decimal import Decimal
from random import randrange, uniform

import pandas as pd

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange_base import ExchangeBase, PriceType
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_query_result import ClientOrderBookQueryResult
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
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class CandleMaker(ScriptStrategyBase):
    """
    This strategy creates random trades on a market with random amounts following the price of one or several sources.
    It is commonly used to create candles on a market with low activity.
    Before running this example, make sure:
      - Run `config rate_oracle_source coinbase`
      - Set Maker market
      - Set Taker market
      - Set conversion_rate
    """

    maker_source_name: str = "coinhub"
    maker_trading_pair: str = "BTC-MNT"
    maker_base_asset, maker_quote_asset = split_hb_trading_pair(maker_trading_pair)
    taker_source_name: str = "binance_paper_trade"
    taker_trading_pair: str = "BTC-USDT"
    taker_base_asset, taker_quote_asset = split_hb_trading_pair(taker_trading_pair)

    conversion_pair: str = f"{taker_quote_asset}-{maker_quote_asset}"
    markets = {maker_source_name: {maker_trading_pair}, taker_source_name: {taker_trading_pair}}

    all_markets_ready = False
    rate_oracle_ready = False
    rate_oracle_task = None
    price_open = None

    create_timestamp = 0

    min_order_refresh_time = 5
    max_order_refresh_time = 20
    subscribed_to_order_book_trade_event: bool = False

    ignore_list = []
    cancel_ignore_list = []

    status_report_interval = 900
    _last_timestamp = 0
    order_size = 0

    _cancel_task = None
    should_report_warnings = 0

    max_order_age = 10

    @property
    def spread(self) -> Decimal:
        return Decimal(self._spread) / Decimal("100")

    @property
    def maker(self) -> ExchangeBase:
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.maker_source_name]

    @property
    def taker(self) -> ExchangeBase:
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.taker_source_name]

    @property
    def order_refresh_time(self) -> int:
        random_refresh_time = randrange(self.min_order_refresh_time, self.max_order_refresh_time)
        return random_refresh_time

    @property
    def min_order_size(self) -> Decimal:
        maker_trading_rule = self.maker.trading_rules[self.maker_trading_pair]
        min_order_size = maker_trading_rule.min_order_size
        return min_order_size

    @property
    def max_order_size(self) -> Decimal:
        maker_trading_rule = self.maker.trading_rules[self.maker_trading_pair]
        max_order_size = maker_trading_rule.min_order_size * 10
        return max_order_size

    @property
    def amplifier_amount(self) -> Decimal:
        return 10

    @property
    def conversion_rate(self) -> Decimal:
        # return RateOracle.get_instance().get_pair_rate(self.conversion_pair)
        return Decimal("3452.0")

    @property
    def is_ready(self) -> bool:
        if not self.all_markets_ready or not self.rate_oracle_ready:
            self.all_markets_ready = all([market.ready for market in [self.maker, self.taker]])
            self.rate_oracle_ready = RateOracle.get_instance().ready
            if not self.rate_oracle_ready:
                if self.should_report_warnings:
                    self.logger().warning("Conversion rates are not ready. No market making trades are permitted.")
                if self.rate_oracle_task is None:
                    self.rate_oracle_task = safe_ensure_future(RateOracle.get_instance().start_network())
                return False
            if not self.all_markets_ready:
                # Markets not ready yet. Don't do anything.
                if self.should_report_warnings:
                    self.logger().warning("Markets are not ready. No market making trades are permitted.")
                return False
            else:
                self.logger().info("Markets are ready. Trading started.")
        return True

    @property
    def taker_last_price(self) -> Decimal:
        return self.taker.get_price_by_type(self.taker_trading_pair, PriceType.LastTrade)

    @property
    def taker_last_price_converted(self) -> Decimal:
        return self.taker_last_price * self.conversion_rate

    @property
    def maker_best_bid(self) -> Decimal:
        return self.maker.get_price_by_type(self.maker_trading_pair, PriceType.BestBid)

    @property
    def maker_best_ask(self) -> Decimal:
        return self.maker.get_price_by_type(self.maker_trading_pair, PriceType.BestAsk)

    @property
    def trade_type(self) -> TradeType:
        if self.price_open is None or (self.price_open is not None and self.taker_last_price > self.price_open):
            return TradeType.BUY
        return TradeType.SELL

    @property
    def volume(self) -> ClientOrderBookQueryResult:
        result = self.maker.get_volume_for_price(self.maker_trading_pair, self.trade_type == TradeType.BUY, self.taker_last_price_converted)
        return Decimal(str(result.result_volume))

    def random_order_size(self) -> Decimal:
        return Decimal(f"{uniform(self.min_order_size.__float__(), self.max_order_size.__float__())}")

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
        if self._cancel_task is None:
            self._cancel_task = safe_ensure_future(self.cancel_all_orders())
        if self.create_timestamp <= self.current_timestamp:
            self.order_size = self.random_order_size()
            if self.should_create_order:
                order_candidate = self.create_order_candidate()
                # Adjust OrderCandidate
                order_adjusted = self.maker.budget_checker.adjust_candidate(order_candidate, all_or_none=False)
                if math.isclose(order_adjusted.amount, Decimal("0"), rel_tol=1E-5):
                    if self.should_report_warnings:
                        self.logger().info(f"Order adjusted: {order_adjusted.amount}, too low to place an order")
                else:
                    self.send_order(order_adjusted)
            self.create_timestamp = self.order_refresh_time + self.current_timestamp
        self._last_timestamp = self.current_timestamp

    @property
    def should_create_order(self) -> bool:
        if len(self.get_active_orders(self.maker_source_name)) == 0:
            if not self.maker_best_ask.is_nan() and not self.maker_best_bid.is_nan():
                if self.maker_best_bid < self.taker_last_price_converted < self.maker_best_ask:
                    return True
                if self.trade_type is TradeType.BUY and self.taker_last_price_converted > self.maker_best_ask and self.volume <= (self.max_order_size * self.amplifier_amount):
                    return True
                if self.trade_type is TradeType.SELL and self.taker_last_price_converted < self.maker_best_bid and self.volume <= (self.max_order_size * self.amplifier_amount):
                    return True
            if self.should_report_warnings:
                self.logger().error("The order should not be created.")
                self.logger().error("Because:")
                if self.maker_best_ask.is_nan():
                    self.logger().error(" - Maker best ask is NaN")
                if self.maker_best_bid.is_nan():
                    self.logger().error(" - Maker best bid is NaN")
                if self.volume > self.order_size:
                    self.logger().error(f"To make candle, it requires {self.volume} volume")
                if not (self.maker_best_bid < self.taker_last_price_converted < self.maker_best_ask):
                    if not self.maker_best_bid < self.taker_last_price_converted:
                        self.logger().error(f" - The price ({self.taker_last_price_converted}) is lower than maker best bid ({self.maker_best_bid})")
                    if not self.maker_best_ask > self.taker_last_price_converted:
                        self.logger().error(f" - The price ({self.taker_last_price_converted}) is higher than maker best ask ({self.maker_best_ask})")
        return False

    def create_order_candidate(self) -> OrderCandidate:
        if self.price_open is None:
            self.price_open = self.taker_last_price
        order_side = self.trade_type
        price = self.taker_last_price_converted
        order_amount = self.order_size
        if self.order_size < self.volume <= (self.max_order_size * self.amplifier_amount):
            order_amount = self.max_order_size * 10
        amount = self.maker.quantize_order_amount(self.maker_trading_pair, order_amount)
        price = self.maker.quantize_order_price(self.maker_trading_pair, price)
        self.price_open = self.taker_last_price
        return OrderCandidate(
            trading_pair=self.maker_trading_pair,
            is_maker = True,
            order_type = OrderType.LIMIT,
            order_side = order_side,
            amount = amount,
            price = price)

    def send_order(self, order: OrderCandidate) -> str:
        """
        Send order to the exchange, indicate that position is filling, and send log message with a trade.
        """
        is_buy = order.order_side == TradeType.BUY
        place_order = self.buy if is_buy else self.sell
        return place_order(
            connector_name=self.maker_source_name,
            trading_pair=self.maker_trading_pair,
            amount=order.amount,
            order_type=order.order_type,
            price=order.price
        )

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        """
        Method called when the connector notifies a buy order has been created
        """
        # self.logger().info(logging.INFO, f"The buy order {event.order_id} has been created")
        if event.order_id not in self.ignore_list and event.order_id not in self.cancel_ignore_list:
            client_order_id = self.send_order(OrderCandidate(trading_pair=event.trading_pair, is_maker=True, order_type=OrderType.LIMIT, order_side=TradeType.SELL, amount=event.amount, price=event.price))
            self.ignore_list.append(client_order_id)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        """
        Method called when the connector notifies a sell order has been created
        """
        # self.logger().info(logging.INFO, f"The sell order {event.order_id} has been created")
        if event.order_id not in self.ignore_list and event.order_id not in self.cancel_ignore_list:
            client_order_id = self.send_order(OrderCandidate(trading_pair=event.trading_pair, is_maker=True, order_type=OrderType.LIMIT, order_side=TradeType.BUY, amount=event.amount, price=event.price))
            self.ignore_list.append(client_order_id)

    def did_fill_order(self, event: OrderFilledEvent):
        self.cancel_ignore_list.append(event.order_id)

    async def cancel_all_orders(self):
        orders = self.get_active_orders(connector_name=self.maker_source_name)
        if self.should_report_warnings:
            self.logger().info(f"CANCEL ACTIVE ORDERS ({len(orders)})")
        for order in orders:
            cancel_timestamp = order.creation_timestamp / 1000000 + self.max_order_age
            if cancel_timestamp < self.current_timestamp:
                self.cancel(self.maker_source_name, order.trading_pair, order.client_order_id)
        self._cancel_task = None

    def subscribe_to_order_book_trade_event(self):
        """
        Subscribe to raw trade event.
        """
        self.order_book_trade_event = SourceInfoEventForwarder(self._process_public_trade)
        for order_book in self.maker.order_books.values():
            order_book.add_listener(OrderBookEvent.TradeEvent, self.order_book_trade_event)
        self.subscribed_to_order_book_trade_event = True

    def _process_public_trade(self, event_tag: int, market: ConnectorBase, event: OrderBookTradeEvent):
        """
        Add new trade to list, remove old trade event, if count greater than trade_count_limit.
        """
        self.create_timestamp = event.timestamp + self.order_refresh_time

    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Exchange", "Market", "Side", "Price", "Amount", "Age"]
        data = []
        for order in self.get_active_orders(self.maker_source_name):
            age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
            data.append([
                self.maker_exchange,
                order.trading_pair,
                "buy" if order.is_buy else "sell",
                float(order.price),
                float(order.quantity),
                age_txt
            ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", "Side"], inplace=True)
        return df
