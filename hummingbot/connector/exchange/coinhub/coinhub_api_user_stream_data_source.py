import asyncio
import json
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.coinhub import coinhub_constants as CONSTANTS
from hummingbot.connector.exchange.coinhub.coinhub_auth import CoinhubAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinhub.coinhub_exchange import CoinhubExchange


class CoinhubAPIUserStreamDataSource(UserStreamTrackerDataSource):

    LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800  # Recommended to Ping/Update listen key to keep connection alive
    HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: CoinhubAuth,
                 trading_pairs: List[str],
                 connector: 'CoinhubExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: CoinhubAuth = auth
        self._current_listen_key = None
        self._domain = domain
        self._api_factory = api_factory
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._listen_key_initialized_event: asyncio.Event = asyncio.Event()
        self._last_listen_key_ping_ts = 0

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """

        await self._listen_key_initialized_event.wait()

        ws: WSAssistant = await self._get_ws_assistant()
        url = f"{CONSTANTS.WSS_URL.format(self._domain)}/{self._current_listen_key}"
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        Binance does not require any channel subscription.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """

        await websocket_assistant.send(json.dumps(self._auth.generate_websocket_subscription()))
        print(self._trading_pairs)
        # await websocket_assistant.send(json.dumps({"op": "subscribe", "channel": "orders"}))
        # await websocket_assistant.send(json.dumps({"op": "subscribe", "channel": "fills"}))

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
