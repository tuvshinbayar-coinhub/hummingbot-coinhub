from decimal import Decimal
from typing import Dict, Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

REST_CALL_RATE_LIMIT_ID = "coinhub_rate_limit_id"
RATE_LIMITS = [RateLimit(REST_CALL_RATE_LIMIT_ID, limit=50, time_interval=60)]

COINHUB_CHANNEL = "coinhub-sandbox"
COINHUB_FIATS_RATE_URL = "https://sandbox-api.coinhub.mn/v1/fiats"


class CoinhubRateSource(RateSourceBase):
    def __init__(self):
        async_throttler = AsyncThrottler(rate_limits=RATE_LIMITS, limits_share_percentage=Decimal("100.0"))
        self._api_factory = WebAssistantsFactory(throttler=async_throttler)
        super().__init__()

    @property
    def name(self) -> str:
        return "coinhub"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        """
        :return A dictionary of trading pairs and prices
        """
        results = {}
        fiats = await self.get_coinhub_prices()
        if isinstance(fiats, Exception):
            self.logger().error(fiats)
            self.logger().error(
                "Unexpected error while retrieving rates from Coinhub. " "Check the log file for more info."
            )
        else:
            print()
            results.update(fiats)
        return results

    async def get_coinhub_prices(self) -> Dict[str, Decimal]:
        """
        Fetches Coinhub exchange rates.
        :return A dictionary of fiat rates
        """
        results = {}
        rest_assistant = await self._api_factory.get_rest_assistant()
        price_url = COINHUB_FIATS_RATE_URL
        request_result = await rest_assistant.execute_request(
            method=RESTMethod.GET,
            url=price_url,
            throttler_limit_id=REST_CALL_RATE_LIMIT_ID,
            params={
                "channel": COINHUB_CHANNEL
            }
        )
        rates = request_result["data"]
        for rate in rates:
            pair = f"{rate['stock'].upper()}-{rate['money'].upper()}"
            results[pair] = Decimal(f"{rate['price']}")
        return results
