import pandas as pd
from gitlab import Gitlab

from prefect import Task


class UpdateGitlabData(Task):
    def __init__(
            self,
            url: str = None,
            access_token_secret: str = None,
            branch: str = None,
            timeout: int = 3600,
            **kwargs
    ):
        self.url = url
        self.access_token_secret = access_token_secret
        self.branch = branch

        super().__init__(name="update_gitlab_data", timeout=timeout, **kwargs)

    def run(
            self, 
            url: str = None
            from_path: str = None
            

    )