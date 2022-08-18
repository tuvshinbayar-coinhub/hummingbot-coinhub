import time
import hashlib
import hmac
import json
from urllib.parse import urlparse, urlencode

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest
from hummingbot.connector.exchange.coinhub import coinhub_constants as CONSTANTS


class CoinhubAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        timestamp = int(time.time() * 1000)
        path_url = urlparse(request.url).path

        if request.method == RESTMethod.POST:
            content_to_sign = f"{timestamp}{request.method}{path_url}"
            encoded_param = urlencode(json.loads(request.data))
            content_to_sign += encoded_param
            if "" == encoded_param:
                content_to_sign += "{}"
        else:
            content_to_sign = f"{timestamp}{request.method}{path_url}"

        signature = hmac.new(
            self.secret_key.encode("utf8"), msg=content_to_sign.encode("utf8"), digestmod=hashlib.sha256
        ).hexdigest()

        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        # v1 Authentication headers
        headers.update({"API-KEY": self.api_key, "SIGN": signature, "TS": str(timestamp)})
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSJSONRequest) -> WSJSONRequest:
        access_id: str = self.api_key
        tonce = int(1000 * time.time())
        content_to_sign = f"{tonce}{RESTMethod.POST}{CONSTANTS.WS_SIGN_PATH_URL}".encode()
        signature = hmac.new(self.secret_key.encode("utf8"), msg=content_to_sign, digestmod=hashlib.sha256).hexdigest()

        subscribe = {"id": "111111", "method": "server.sign", "params": [access_id, signature, tonce]}

        request.payload.update(subscribe)

        return request
