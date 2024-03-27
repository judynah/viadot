import pandas as pd
# from gitlab import Gitlab
import re
from prefect import Task
from gitlab.exceptions import GitlabHttpError,GitlabGetError
from prefect.utilities.tasks import defaults_from_attrs




class UpdateWiki(Task):
    def __init__(
        self,
        url: str,
        credentials: str = None,
        config_key: str =  "Gitlab",
        timeout: int = 3600,
        *args,
        **kwargs
    ):
        
        """Task for updating Gitlab WikiPage.
        
        Args:
            url (str, required): URL adress to repository in the Gitlab in form  `https://gitlab.(organization).com/repo`. Defaults to None.
            credentials (Dict[str, Any], optional): Credentials stored in a dictionary. Required credentials: token. Defaults to None.
            config_key (str, optional): Credential key to dictionary where details are stored. Defaults to None.
            timeout (int, optional): The amount of time (in seconds) to wait while running this task before a timeout occurs. Defaults to 3600.
            
        Returns:
            ValueError: If provided url not found.

        """
        
        self.url = url
        if self.url is None:
            raise ValidationError("No valid url found.")
        
        self.config_key = config_key
        self.credentials = credentials
        
        super().__init__(
            name="update_gitlab_data", 
            timeout=timeout, 
            *args,
            **kwargs)

    def run(
            self, 
            wiki_path: str,
            wiki_content: str = "",
            file_path: str = None,
            timeout: int = 3600,
    ) -> dict:
        """Task run method.
        
            Args:
                wiki_path (str, required): Path to wiki page that derived from url to the wiki page after `wikis` ex. `/home/Entities/Automatio/data/view`.
                wiki_content (str, optional): New content of wiki page. Default to None.
                file_path (str, optional): Path to the file, that has to be uploaded to the Wiki Page. Default to None.
                timeout (int, optional): The amount of time (in seconds) to wait while running this task before a timeout occurs. Defaults to 3600.
                
            Returns:
                Dict: Returns a dictonary with page attributes.
        """
        
        gitlab_url = re.search(".*gitlab\.[^/]*/", self.url).group()
        project_namespace = self.url.replace(gitlab_url, "")

        gitlab = Gitlab(url = gitlab_url, credentials=self.credentials, config_key=self.config_key)
        gl = gitlab.get_conn()
        # gitlab.auth()
        
        project = gl.projects.get(project_namespace)
        page = project.wikis.get(wiki_path)

        
        # try:
        #     page = project.wikis.get(wiki_path)
        # except (GitlabHttpError, GitlabGetError):
        #     print("Wiki Page does not exist")
            # page = project.wikis.create({'title': wiki_path, 
            #                             'content': None})
            

        page.content = wiki_content
        if file_path is not None:
            temp = page.upload(filename = file_path.split("/")[-1], filepath=file_path)
            wiki_content = page.content + '\n\n'+ temp['link']['markdown']
        page.content = wiki_content
        page.title = page.slug
        page.save()

        return page.attributes
    
    
    
class GetWiki(Task):
    def __init__(
        self,
        url: str,
        credentials: str = None,
        config_key: str = "Gitlab",
        timeout: int = 3600,
        *args,
        **kwargs
    ):
        """Task for getting Gitlab WikiPage.
        
            Args:
                url (str, required): URL adress to repository in the Gitlab in form  `https://gitlab.(organization).com/repo`. Defaults to None.
                credentials (Dict[str, Any], optional): Credentials stored in a dictionary. Required credentials: token. Defaults to None.
                config_key (str, optional): Credential key to dictionary where details are stored. Defaults to None.
                timeout (int, optional): The amount of time (in seconds) to wait while running this task before a timeout occurs. Defaults to 3600.
                
            Returns:
                Str: Returns content of wiki page in string format.
       
        """
        

        self.url = url
        self.config_key = config_key
        self.credentials = credentials

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
            wiki_path: str,
            wiki_list: bool = False,
            timeout: int = 3600,
    ) -> str:
        
        """Task run method.
        
            Args:
                wiki_path (str, required): Path to wiki page that derived from url to the wiki page after `wikis` ex. `/home/Entities/Automatio/data/view`.
                wiki_list (bool, optional): Indicator if list of wiki page has to be received. Default to False.
                timeout (int, optional): The amount of time (in seconds) to wait while running this task before a timeout occurs. Defaults to 3600.
                
            Returns:
                Str: Content of wiki page.

        """
        
        self.wiki_path=wiki_path
        self.wiki_list = wiki_list
        self.wiki_sidebar = wiki_sidebar
        
        if self.url==None:
            raise ValueError("URL are required")
            
        if self.wiki_path==None:
            raise ValueError("Wiki path are required")
        
        gitlab_url = re.search(".*gitlab\.[^/]*/", self.url).group()
        project_namespace = self.url.replace(gitlab_url, "")

        gitlab = Gitlab(url = gitlab_url, credentials=self.credentials, config_key=self.config_key)
        gl = gitlab.get_conn()
        # gitlab.auth()
        
        project = gl.projects.get(project_namespace)
        
        if self.wiki_list==True:
            return project.wikis.list()
            

        page = project.wikis.get(wiki_path)

        return page.attributes['content']
            