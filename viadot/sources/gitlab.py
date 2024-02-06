from typing import Any, Dict, Literal
import gitlab

from viadot.sources.base import Source
from viadot.config import local_config
from viadot.exceptions import CredentialError


class Gitlab(Source):
    """
    Source for getting data from Gitlab API.

    """

    def __init__(
        self,
        config_key: str = "Gitlab",
        credentials: Dict[str, Any] = None,
        url: str = None,
        *args,
        **kwargs
    ):
        """
        Creating an instance of Gitlab source class.
        """

        if config_key:
            config_credentials = local_config.get(config_key)

        credentials = credentials if credentials else config_credentials
        if credentials is None:
            raise CredentialError("Credentials not found.")

        self.config_key = config_key
        self.url = url

        super().__init__(*args, credentials=credentials, **kwargs)

    def get_conn(self, url):
        return gitlab.Gitlab(
            url=self.url, private_token=self.credentials.get("private_token")
        )
