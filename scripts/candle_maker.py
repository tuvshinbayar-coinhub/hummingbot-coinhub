import math
from decimal import Decimal
from random import randrange, uniform

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange_base import ExchangeBase, PriceType
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    OrderBookEvent,
    OrderBookTradeEvent,
    OrderType,
    SellOrderCreatedEvent,
)
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class RateMaker(ScriptStrategyBase):
    """
    THis strategy buys ETH (with BTC) when the ETH-BTC drops 5% below 50 days moving average (of a previous candle)
    This example demonstrates:
      - How to call Binance REST API for candle stick data
      - How to incorporate external pricing source (Coingecko) into the strategy
      - How to listen to order filled event
      - How to structure order execution on a more complex strategy
    Before running this example, make sure you run `config rate_oracle_source coingecko`
    """

    maker_source_name: str = "coinhub_sandbox"
    maker_trading_pair: str = "ETH-MNT"
    maker_base_asset, maker_quote_asset = split_hb_trading_pair(maker_trading_pair)
    taker_source_name: str = "binance_paper_trade"
    taker_trading_pair: str = "ETH-USDT"
    taker_base_asset, taker_quote_asset = split_hb_trading_pair(taker_trading_pair)

    conversion_pair: str = f"{taker_quote_asset}-{maker_quote_asset}"
    markets = {maker_source_name: {maker_trading_pair}, taker_source_name: {taker_trading_pair}}

    all_markets_ready = False
    rate_oracle_ready = False
    rate_oracle_task = None
    price_open = None

    create_timestamp = 0

    min_order_refresh_time = 5
    max_order_refresh_time = 15
    subscribed_to_order_book_trade_event: bool = False

    ignore_list = []

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
        return randrange(self.min_order_refresh_time, self.max_order_refresh_time)

    @property
    def order_size(self) -> Decimal:
        maker_trading_rule = self.maker.trading_rules[self.maker_trading_pair]
        min_order_size = maker_trading_rule.min_order_size
        max_order_size = maker_trading_rule.min_order_size * 10
        return Decimal(f"{uniform(min_order_size.__float__(), max_order_size.__float__())}")

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
                self.logger().warning("Conversion rates are not ready. No market making trades are permitted.")
                if self.rate_oracle_task is None:
                    self.rate_oracle_task = safe_ensure_future(RateOracle.get_instance().start_network())
                return False
            if not self.all_markets_ready:
                # Markets not ready yet. Don't do anything.
                self.logger().warning("Markets are not ready. No market making trades are permitted.")
                return False
            else:
                self.logger().info("Markets are ready. Trading started.")
        return True

    def on_tick(self):
        """
        Runs every tick_size seconds, this is the main operation of the strategy.
        - Create proposal (a list of order candidates)
        - Check the account balance and adjust the proposal accordingly (lower order amount if needed)
        - Lastly, execute the proposal on the exchange
        """
        if not self.is_ready:
            return
        if not self.subscribed_to_order_book_trade_event:
            # Set pandas resample rule for a timeframe
            self.subscribe_to_order_book_trade_event()
        self.cancel_all_orders()
        if self.create_timestamp <= self.current_timestamp:
            order_candidate = self.create_order_candidate()
            # Adjust OrderCandidate
            order_adjusted = self.maker.budget_checker.adjust_candidate(order_candidate, all_or_none=False)
            if math.isclose(order_adjusted.amount, Decimal("0"), rel_tol=1E-5):
                self.logger().info(f"Order adjusted: {order_adjusted.amount}, too low to place an order")
            else:
                self.send_order(order_adjusted)
            self.create_timestamp = self.order_refresh_time + self.current_timestamp

    def create_order_candidate(self) -> OrderCandidate:
        taker_last_price = self.taker.get_price_by_type(self.taker_trading_pair, PriceType.LastTrade)
        if self.price_open is None:
            self.price_open = taker_last_price
        price_close = taker_last_price
        order_side = TradeType.BUY if price_close > self.price_open else TradeType.SELL
        price = self.conversion_rate * taker_last_price
        amount = self.maker.quantize_order_amount(self.maker_trading_pair, self.order_size)
        price = self.maker.quantize_order_price(self.maker_trading_pair, price)

        return OrderCandidate(
            trading_pair=self.maker_trading_pair,
            is_maker = True,
            order_type = OrderType.LIMIT,
            order_side = order_side,
            amount = amount,
            price = price)

    def send_order(self, order: OrderCandidate):
        """
        Send order to the exchange, indicate that position is filling, and send log message with a trade.
        """
        is_buy = order.order_side == TradeType.BUY
        place_order = self.buy if is_buy else self.sell
        place_order(
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
        if event.order_id not in self.ignore_list:
            client_order_id = self.maker.sell(self.maker_trading_pair, event.amount, OrderType.LIMIT, event.price)
            self.ignore_list.append(client_order_id)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        """
        Method called when the connector notifies a sell order has been created
        """
        # self.logger().info(logging.INFO, f"The sell order {event.order_id} has been created")
        if event.order_id not in self.ignore_list:
            client_order_id = self.maker.buy(self.maker_trading_pair, event.amount, OrderType.LIMIT, event.price)
            self.ignore_list.append(client_order_id)

    def cancel_all_orders(self):
        orders = self.get_active_orders(connector_name=self.maker_source_name)
        for order in orders:
            self.cancel(self.maker_source_name, order.trading_pair, order.client_order_id)

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
        self.logger().info("### Subscription event ###")
        self.logger().info(event)
        # self.logger().info(f"current_time: {self.current_timestamp}")airdrop

        self.create_timestamp = event.timestamp + self.order_refresh_time
