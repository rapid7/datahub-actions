import logging
from typing import Optional

import requests
from pydantic import SecretStr
from requests.models import HTTPBasicAuth, HTTPError

logger = logging.getLogger(__name__)


class ApiRequest:
    def __int__(self):
        self.session = requests.session()

    def make_request(
        self,
        endpoint: str,
        data: Optional[dict],
        api_key: SecretStr,
        secret_key: SecretStr,
    ) -> None:
        try:
            assert api_key.get_secret_value().strip(), "Api Key is required"
            assert secret_key.get_secret_value().strip(), "Secret Key is required"

            response = self.session.put(
                url=endpoint,
                data=data,
                auth=HTTPBasicAuth(
                    api_key.get_secret_value(), secret_key.get_secret_value()
                ),
            )
            response.raise_for_status()
        except HTTPError as error:
            logger.error(str(error))
