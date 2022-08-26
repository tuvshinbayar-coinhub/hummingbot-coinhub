import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.coinhub import (
    coinhub_constants as CONSTANTS,
    coinhub_utils,
    coinhub_web_utils as web_utils,
)
from hummingbot.connector.exchange.coinhub.coinhub_api_order_book_data_source import CoinhubAPIOrderBookDataSource
from hummingbot.connector.exchange.coinhub.coinhub_api_user_stream_data_source import CoinhubAPIUserStreamDataSource
from hummingbot.connector.exchange.coinhub.coinhub_auth import CoinhubAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_logger = None


class CoinhubExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        coinhub_api_key: str,
        coinhub_api_secret: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = coinhub_api_key
        self.secret_key = coinhub_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_coinhub_timestamp = 1.0
        super().__init__(client_config_map)

    @staticmethod
    def coinhub_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(binance_type: str) -> OrderType:
        return OrderType[binance_type]

    @property
    def authenticator(self):
        return CoinhubAuth(api_key=self.api_key, secret_key=self.secret_key)

    @property
    def name(self) -> str:
        return "coinhub"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.MARKET_LIST_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.MARKET_LIST_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    async def _api_request(self,
                           path_url,
                           endpoint: str = CONSTANTS.DEFAULT_ENDPOINT,
                           method: RESTMethod = RESTMethod.GET,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None,
                           is_auth_required: bool = False,
                           return_err: bool = False,
                           limit_id: Optional[str] = None) -> Dict[str, Any]:

        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        if is_auth_required:
            url = self.web_utils.private_rest_url(path_url, endpoint=endpoint, domain=self.domain)
        else:
            url = self.web_utils.public_rest_url(path_url, endpoint=endpoint, domain=self.domain)

        return await rest_assistant.execute_request(
            url=url,
            params=params,
            data=data,
            method=method,
            is_auth_required=is_auth_required,
            return_err=return_err,
            throttler_limit_id=limit_id if limit_id else path_url,
        )

    async def check_network(self) -> NetworkStatus:
        """
        Checks connectivity with the exchange using the API
        """
        try:
            await self._api_get(path_url=self.check_network_request_path, endpoint=CONSTANTS.PUBLIC_API_ENDPOINT)
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler, time_synchronizer=self._time_synchronizer, domain=self._domain, auth=self._auth
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CoinhubAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinhubAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
    ) -> Tuple[str, float]:
        order_result = None
        amount_str = f"{amount:f}"
        price_str = f"{price:f}"
        side = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {
            "amount": amount_str,
            "client_id": CONSTANTS.API_CLIENT_ID,
            "market": symbol,
            "price": price_str,
            "side": side,
        }
        order_resp = await self._api_post(path_url=CONSTANTS.CREATE_ORDER_PATH_URL, data=api_params, is_auth_required=True)
        order_result = order_resp["data"]
        o_id = str(order_result["id"])
        transact_time = order_result["mtime"] * 1e-3
        return (o_id, transact_time)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        api_params = {
            "market": symbol,
            "order_id": tracked_order.exchange_order_id
        }
        cancel_resp = await self._api_post(
            path_url=CONSTANTS.ORDER_CANCEL_PATH_URL, data=api_params, is_auth_required=True
        )
        cancel_result = cancel_resp["data"]
        if not cancel_resp["data"] and cancel_resp["code"] == 10:
            return True
        if cancel_result and cancel_result["status"] == "done":
            return True
        return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Example:
        {
            "code": 200,
            "message": "success",
            "data": [
                {
                    "market": "CHB/MNT",
                    "money": "MNT",
                    "stock": "CHB",
                    "moneyPrec": 4,
                    "stockPrec": 4,
                    "feePrec": 4,
                    "minAmount": 1,
                    "type": 1,
                    "canTrade": true
                },
                {
                    "market": "WPL/MNT",
                    "money": "MNT",
                    "stock": "WPL",
                    "moneyPrec": 5,
                    "stockPrec": 1,
                    "feePrec": 4,
                    "minAmount": 500,
                    "type": 1,
                    "canTrade": true
                }
            ]
        }
        """
        trading_pair_rules = exchange_info_dict.get("data", [])
        retval = []
        for rule in filter(coinhub_utils.is_exchange_information_valid, trading_pair_rules):
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule.get("market"))

                min_order_size = Decimal(rule.get("minAmount"))
                min_amount_inc = Decimal(f"1e-{rule['stockPrec']}")
                min_price_inc = Decimal(f"1e-{rule['moneyPrec']}")

                retval.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=min_order_size,
                        min_price_increment=min_price_inc,
                        min_base_amount_increment=min_amount_inc,
                    )
                )

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return retval

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                )
                self._last_poll_timestamp = self.current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(e)
                self.logger().network("Unexpected error while polling updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch updates from Coinhub-Sandbox. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(5.0)

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("e")
                # Refer to https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md
                # As per the order update section in Binance the ID of the order being canceled is under the "C" key
                if event_type == "executionReport":
                    execution_type = event_message.get("x")
                    if execution_type != "CANCELED":
                        client_order_id = event_message.get("c")
                    else:
                        client_order_id = event_message.get("C")

                    if execution_type == "TRADE":
                        tracked_order = self._order_tracker.fetch_order(client_order_id=client_order_id)
                        if tracked_order is not None:
                            fee = TradeFeeBase.new_spot_fee(
                                fee_schema=self.trade_fee_schema(),
                                trade_type=tracked_order.trade_type,
                                percent_token=event_message["N"],
                                flat_fees=[TokenAmount(amount=Decimal(event_message["n"]), token=event_message["N"])],
                            )
                            trade_update = TradeUpdate(
                                trade_id=str(event_message["t"]),
                                client_order_id=client_order_id,
                                exchange_order_id=str(event_message["i"]),
                                trading_pair=tracked_order.trading_pair,
                                fee=fee,
                                fill_base_amount=Decimal(event_message["l"]),
                                fill_quote_amount=Decimal(event_message["l"]) * Decimal(event_message["L"]),
                                fill_price=Decimal(event_message["L"]),
                                fill_timestamp=event_message["T"] * 1e-3,
                            )
                            self._order_tracker.process_trade_update(trade_update)

                    tracked_order = self.in_flight_orders.get(client_order_id)
                    if tracked_order is not None:
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=event_message["E"] * 1e-3,
                            new_state=CONSTANTS.ORDER_STATE[event_message["X"]],
                            client_order_id=client_order_id,
                            exchange_order_id=str(event_message["i"]),
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

                elif event_type == "outboundAccountPosition":
                    balances = event_message["B"]
                    for balance_entry in balances:
                        asset_name = balance_entry["a"]
                        free_balance = Decimal(balance_entry["f"])
                        total_balance = Decimal(balance_entry["f"]) + Decimal(balance_entry["l"])
                        self._account_available_balances[asset_name] = free_balance
                        self._account_balances[asset_name] = total_balance

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _update_order_fills_from_trades(self):
        """
        This is intended to be a backup measure to get filled events with trade ID for orders,
        in case Binance's user stream events are not working.
        NOTE: It is not required to copy this functionality in other connectors.
        This is separated from _update_order_status which only updates the order status without producing filled
        events, since Binance's get order endpoint does not return trade IDs.
        The minimum poll interval for order status is 10 seconds.
        """
        small_interval_last_tick = self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        small_interval_current_tick = self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        long_interval_last_tick = self._last_poll_timestamp / self.LONG_POLL_INTERVAL
        long_interval_current_tick = self.current_timestamp / self.LONG_POLL_INTERVAL

        if long_interval_current_tick > long_interval_last_tick or (
            self.in_flight_orders and small_interval_current_tick > small_interval_last_tick
        ):
            # query_time = int(self._last_trades_poll_coinhub_timestamp * 1e3)
            self._last_trades_poll_coinhub_timestamp = self._time_synchronizer.time()
            order_by_exchange_id_map = {}
            for order in self._order_tracker.all_orders.values():
                order_by_exchange_id_map[order.exchange_order_id] = order

            tasks = []
            trading_pairs = self.trading_pairs
            for trading_pair in trading_pairs:
                params = {"market": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair), "limit": 100, "offset": 0}
                # if self._last_poll_timestamp > 0:
                #     params["startTime"] = query_time
                tasks.append(self._api_post(path_url=CONSTANTS.MY_TRADES_PATH_URL, data=params, is_auth_required=True))

            self.logger().debug(f"Polling for order fills of {len(tasks)} trading pairs.")
            results = await safe_gather(*tasks, return_exceptions=True)

            for trades, trading_pair in zip(results, trading_pairs):
                symbol = (await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair))
                if isinstance(trades, Exception):
                    self.logger().network(
                        f"Error fetching trades update for the order {trading_pair}: {trades}.",
                        app_warning_msg=f"Failed to fetch trade update for {trading_pair}.",
                    )
                    continue
                for trade in trades["data"]["records"]:
                    exchange_order_id = str(trade["id"])
                    if exchange_order_id in order_by_exchange_id_map:
                        # This is a fill for a tracked order
                        tracked_order = order_by_exchange_id_map[exchange_order_id]
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=tracked_order.trade_type,
                            percent_token=symbol.split("/")[0] if trade["side"] == 2 else symbol.split("/")[1],
                            flat_fees=[
                                TokenAmount(amount=Decimal(trade["deal_fee"]), token=symbol.split("/")[0] if trade["side"] == 2 else symbol.split("/")[1])
                            ],
                        )
                        trade_update = TradeUpdate(
                            trade_id=str(trade["id"]),
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=exchange_order_id,
                            trading_pair=trading_pair,
                            fee=fee,
                            fill_base_amount=Decimal(trade["deal_stock"]),
                            fill_quote_amount=Decimal(trade["deal_money"]),
                            fill_price=Decimal(trade["price"]),
                            fill_timestamp=trade["ftime"] * 1e-3,
                        )
                        self._order_tracker.process_trade_update(trade_update)
                    elif self.is_confirmed_new_order_filled_event(str(trade["id"]), exchange_order_id, trading_pair):
                        # This is a fill of an order registered in the DB but not tracked any more
                        self._current_trade_fills.add(
                            TradeFillOrderDetails(
                                market=self.display_name, exchange_trade_id=str(trade["id"]), symbol=trading_pair
                            )
                        )
                        self.trigger_event(
                            MarketEvent.OrderFilled,
                            OrderFilledEvent(
                                timestamp=float(trade["time"]) * 1e-3,
                                order_id=self._exchange_order_ids.get(str(trade["id"]), None),
                                trading_pair=trading_pair,
                                trade_type=TradeType.BUY if trade["side"] == 2 else TradeType.SELL,
                                order_type=OrderType.LIMIT,
                                price=Decimal(trade["price"]),
                                amount=Decimal(trade["deal_stock"]),
                                trade_fee=DeductedFromReturnsTradeFee(
                                    flat_fees=[TokenAmount(symbol.split("/")[0] if trade["side"] == 2 else symbol.split("/")[1], Decimal(trade["deal_fee"]))]
                                ),
                                exchange_trade_id=str(trade["id"]),
                            ),
                        )
                        self.logger().info(f"Recreating missing trade in TradeFill: {trade}")

    async def _update_order_status(self):
        # This is intended to be a backup measure to close straggler orders, in case Binance's user stream events
        # are not working.
        # The minimum poll interval for order status is 10 seconds.
        last_tick = self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        current_tick = self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL

        tracked_orders: List[InFlightOrder] = list(self.in_flight_orders.values())
        if current_tick > last_tick and len(tracked_orders) > 0:

            tasks = [
                self._api_post(
                    path_url=CONSTANTS.GET_ORDER_PATH_URL,
                    data={
                        "market": await self.exchange_symbol_associated_to_pair(trading_pair=o.trading_pair),
                        "order_id": o.exchange_order_id,
                    },
                    is_auth_required=True,
                )
                for o in tracked_orders
            ]
            self.logger().debug(f"Polling for order status updates of {len(tasks)} orders.")
            results = await safe_gather(*tasks, return_exceptions=True)
            for order_update, tracked_order in zip(results, tracked_orders):
                client_order_id = tracked_order.client_order_id
                exchange_order_id = tracked_order.exchange_order_id

                # If the order has already been canceled or has failed do nothing
                if client_order_id not in self.in_flight_orders:
                    continue

                if isinstance(order_update, Exception):
                    self.logger().network(
                        f"Error fetching status update for the order {exchange_order_id}: {order_update}.",
                        app_warning_msg=f"Failed to fetch status update for the order {exchange_order_id}.",
                    )
                    # Wait until the order not found error have repeated a few times before actually treating
                    # it as failed. See: https://github.com/CoinAlpha/hummingbot/issues/601
                    await self._order_tracker.process_order_not_found(client_order_id)

                else:
                    # Update order execution status
                    update_timestamp = time.time()
                    if "data" in order_update:
                        if "data" in order_update["data"] and order_update["data"]["code"] == 501:
                            new_state = OrderState.CANCELED
                        else:
                            if "data" in order_update["data"]:
                                new_state = CONSTANTS.ORDER_STATE[order_update["data"]["data"]["status"]]
                                if new_state == OrderState.OPEN and Decimal(order_update["data"]["data"]["deal_stock"]) > Decimal("0"):
                                    new_state = OrderState.PARTIALLY_FILLED
                                    update_timestamp = order_update["data"]["data"]["ftime"]
                            else:
                                new_state = CONSTANTS.ORDER_STATE[order_update["data"]["status"]]
                                if new_state == OrderState.OPEN and Decimal(order_update["data"]["deal_stock"]) > Decimal("0"):
                                    new_state = OrderState.PARTIALLY_FILLED
                                    update_timestamp = order_update["data"]["ftime"]
                    else:
                        new_state = OrderState.FAILED

                    update = OrderUpdate(
                        client_order_id=client_order_id,
                        exchange_order_id=exchange_order_id,
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=update_timestamp,
                        new_state=new_state,
                    )
                    self._order_tracker.process_order_update(update)

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_post(path_url=CONSTANTS.ACCOUNTS_PATH_URL, data={}, is_auth_required=True)

        balances = account_info["data"]
        for asset_name, balance_entry in balances.items():
            free_balance = Decimal(balance_entry["available"])
            total_balance = Decimal(balance_entry["available"]) + Decimal(balance_entry["freeze"])
            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)
        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in filter(coinhub_utils.is_exchange_information_valid, exchange_info.get("data", [])):
            mapping[symbol_data["market"]] = combine_to_hb_trading_pair(
                base=symbol_data["stock"], quote=symbol_data["money"]
            )
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        # params = {"symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)}

        resp_json = await self._api_request(
            method=RESTMethod.GET, path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL
        )
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        return float(resp_json["data"][symbol]["close"])
