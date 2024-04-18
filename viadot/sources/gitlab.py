from typing import Any, Dict, Literal
import gitlab
from viadot.sources.base import Source
from viadot.config import local_config
from viadot.exceptions import CredentialError
# from gitlab import Gitla

class Gitlab(Source):

    """
    Source for retrieving and updateing data using the Gitlab API.
    
    """
    def __init__(
        self,
        url: str,
        config_key: str = "GITLAB",
        credentials: Dict[str, Any] = None,
        *args, 
        **kwargs
        ):
        """a
            Creating an instance of Gitlab source class.
            
            Args:
                config_key (str, optional): Credential key to dictionary where credentials are stored. Defaults to "Gitlab".
                credentials (Dict[str, Any], optional): Credentials stored in a dictionary. Defaults to None.
                url (str, required): URL adress to private gitlab repository in the  `https://gitlab.(organization).com/repo`. Defaults to None.
                
            Raises:
                CredentialError: If provided credentials are missing.
                ValueError: If url are missing or has not appropriate form.
        """
        
        DEFAULT_CREDENTIALS = local_config.get(config_key)
        self.credentials = credentials or DEFAULT_CREDENTIALS

        if self.credentials is None:
            raise CredentialError("Credentials not found.")
  
        self.url = url
        if self.url is None:
            raise ValueError("URL is required.")

        super().__init__(*args, credentials=self.credentials, **kwargs)
        
    """
        Creating the connection to Gitlab.
    """
    def get_conn(self):
        return gitlab.Gitlab(
            url = self.url,
            private_token=self.credentials['token'])
        
       
