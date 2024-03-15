import pandas as pd
# from gitlab import Gitlab
import re
from prefect import Task
from gitlab.exceptions import GitlabHttpError,GitlabGetError
from prefect.utilities.tasks import defaults_from_attrs


class UpdateWiki(Task):
    def __init__(
        self,
        url: str = None,
        access_token_secret: str = None,
        branch: str = None,
        timeout: int = 3600,
        *args,
        **kwargs
    ):

        self.url = url
        self.branch = branch
        self.access_token_secret=access_token_secret

        super().__init__(
            name="update_gitlab_data", 
            timeout=timeout, 
            *args,
            **kwargs)

    def run(
            self, 
            url: str = None,
            wiki_path: str = None,
            wiki_content: str = None,
            file_path: str = None,
            branch: str = None,
            timeout: int = 3600,
    ):
        
        gitlab_url = re.search(".*gitlab\.[^/]*/", self.url).group()
        project_namespace = self.url.replace(gitlab_url, "")

        gitlab = Gitlab(url = gitlab_url, credentials=self.access_token_secret)
        gl = gitlab.get_conn()
        # gitlab.auth()

        project = gl.projects.get(project_namespace)
        
        # if wiki_path is None:
        #     raise NameError("Wiki page not found.")
            
        try:
            page = project.wikis.get(wiki_path)
        except (GitlabHttpError, GitlabGetError, NameError):
            print("Wrong wiki page path. Create new wiki page")
            page = project.wikis.create({'title': wiki_path, 
                                        'content': None})
            
    
            
        page.content = wiki_content
        if file_path is not None:
            temp = page.upload(file_name = file_name.split("/")[-1], file_path=file_path)
            wiki_content = page.content + '\n\n'+ temp['link']['markdown']
        page.content = wiki_content
        page.title = page.slug
        page.save()

        return page.attributes
    
    
    
class GetWiki(Task):
    def __init__(
        self,
        url: str = None,
        access_token_secret: str = None,
        branch: str = None,
        timeout: int = 3600,
        *args,
        **kwargs
    ):

        self.url = url
        self.branch = branch
        self.access_token_secret=access_token_secret

        super().__init__(
            name="get_wiki", 
            timeout=timeout, 
            *args,
            **kwargs)
    
    # @defaults_from_attrs(
    #     "url",
    #     "wiki_path"
    # )
    def run(
            self, 
            url: str = None,
            wiki_path: str = None,
            branch: str = None,
            timeout: int = 3600,
    ) -> str:
        
        gitlab_url = re.search(".*gitlab\.[^/]*/", self.url).group()
        project_namespace = self.url.replace(gitlab_url, "")

        gitlab = Gitlab(url = gitlab_url, credentials=self.access_token_secret)
        gl = gitlab.get_conn()
        # gitlab.auth()

        project = gl.projects.get(project_namespace)
        
        # if wiki_path is None:
        #     raise NameError("Wiki page not found.")
            
        try:
            page = project.wikis.get(wiki_path)
        except (GitlabHttpError, GitlabGetError, NameError):
            print("Wrong wiki page path. Create new wiki page")
            # page = project.wikis.create({'title': wiki_path, 
                                        # 'content': None})

        return page.attributes['content']
            
        

