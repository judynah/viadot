import pandas as pd
from gitlab import Gitlab
import re

from prefect import Task


class UpdateWiki(Task):
    def __init__(
        self,
        url: str = None,
        wiki_path: str = None,
        wiki_content: str = None,
        file_path: str = None,
        if_mr: bool = False,
        access_token_secret: str = None,
        branch: str = None,
        timeout: int = 3600,
        *args,
        **kwargs
    ):

        self.url = url
        self.wiki_path = wiki_path
        self.wiki_content = wiki_content
        self.file_path = file_path
        self.access_token_secret = access_token_secret
        self.branch = branch

        super().__init__(name="update_gitlab_data", timeout=timeout, *args, **kwargs)

        def merge_request():
            pass

        def run(
            self,
            url: str = None,
            wiki_path: str = None,
            wiki_content: str = None,
            file_path: str = None,
            if_mr: bool = False,
            access_token_secret: str = None,
            branch: str = None,
            timeout: int = 3600,
        ):

            gitlab_url = re.search(".*gitlab\.[^/]*/", url).group()
            project_namespace = url.replace(gitlab_url, "")

            gl = Gitlab(url=gitlab_url, private_token=access_token_secret)
            gl.auth()

            project = gl.projects.get(project_namespace)

            page = project.wikis.get(wiki_path)
            page.content = wiki_content
            page.title = wiki_path
