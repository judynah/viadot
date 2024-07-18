import json
from typing import Any, Dict
from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import handle_api_response
from viadot.exceptions import CredentialError

class BusinessCore(Source):
    """
    Source for getting data from Bussines Core ERP API.

    """
    def __init__(
        self,
        url: str = None,
        filters_dict: Dict[str, Any] = {
            "BucketCount": None,
            "BucketNo": None,
            "FromDate": None,
            "ToDate": None,
        },
        verify: bool = True,
        credentials: Dict[str, Any] = None,
        config_key: str = "BusinessCore",
        *args,
        **kwargs,
    ):
        """
        Creating an instance of BusinessCore source class.

        Args:
            url (str, optional): Base url to a view in Business Core API. Defaults to None.
            filters_dict (Dict[str, Any], optional): Filters in form of dictionary. Available filters: 'BucketCount',
                'BucketNo', 'FromDate', 'ToDate'.  Defaults to {"BucketCount": None,"BucketNo": None,"FromDate": None,
                "ToDate": None,}.
            verify (bool, optional): Whether or not verify certificates while connecting to an API. Defaults to True.
            credentials (Dict[str, Any], optional): Credentials stored in a dictionary. Required credentials: username,
                password. Defaults to None.
            config_key (str, optional): Credential key to dictionary where details are stored. Defaults to "BusinessCore".

        Raises:
            CredentialError: When credentials are not found.
        """

        credentials = (credentials or get_source_credentials(config_key))
        if credentials is None:
            raise CredentialError("Please specify the credentials.")

        self.url = url
        self.filters_dict = filters_dict
        self.verify = verify

        super().__init__(*args, credentials=credentials, **kwargs)

    @property
    def generate_token(self) -> str:
        """
        Function for generating Business Core API token based on username and password.

        Returns:
        string: Business Core API token.

        """
        url = "https://api.businesscore.ae/api/user/Login"

        payload = f'grant_type=password&username={self.credentials.get("username")}&password={self.credentials.get("password")}&scope='
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = handle_api_response(
            url=url, headers=headers, method="GET", body=payload, verify=self.verify
        )
        token = json.loads(response.text).get("access_token")
        self.token = token
        return token
