from typing import Any, Dict, Literal

from .base import Source
from ..config import local_config
from ..exceptions import CredentialError


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



    def get_data(self):
        response = 
