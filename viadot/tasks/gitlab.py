import pandas as pd
# from viadot.sources import Gitlab
import re
from prefect import Task
from gitlab.exceptions import GitlabHttpError,GitlabGetError
from prefect.utilities.tasks import defaults_from_attrs




class UpdateWiki(Task):
    def __init__(
        self,
        url: str,
        credentials: str = None,
        # config_key: str =  "Gitlab", 
        config_key: str =  "GITLAB", 
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
            wiki_title: str="",
            attachment_path: str = None,
            add_page: bool = False,
            add_sidebar: bool = False,
            replace_content: bool = True,
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
        gl.auth()
        
        project = gl.projects.get(project_namespace)
        # page = project.wikis.get(wiki_path)

        if add_page == True:
            try:
                page = project.wikis.get(wiki_path)
            except (GitlabHttpError, GitlabGetError):
                # print("Wiki Page does not exist")
                page = project.wikis.create({'title': wiki_path, 'content': None}) # gitlab library bug - needs to add wiki_path as title
                if add_sidebar == True: 
                    # splitted_path = wiki_path.split("/")
                    splitted_path = wiki_path.replace("_", " ").strip("/").split('/')[1:-1]
                    page_sidebar = project.wikis.get("_sidebar")
                    pattern=""
                    for el in splitted_path:
                        if el!=splitted_path[-1]:
                            pattern=pattern+f"<summary>{el}</summary>.*?"
                        else:
                            pattern=pattern+f"<summary>{el}</summary>"
                    try:
                        sidebar_content = re.search(pattern, page_sidebar.content, flags = re.DOTALL | re.IGNORECASE).group(0)
                        new_content = sidebar_content+"\n\n["+wiki_path.split('/')[-1]+"]("+wiki_path+")"
                        page_sidebar.content = re.sub(re.escape(sidebar_content), new_content, page_sidebar.content, flags=re.IGNORECASE)
                        page_sidebar.title = page_sidebar.slug
                        page_sidebar.save()
                    except(TypeError, AttributeError):
                        print("No available path. Sidebar not created")
                        # page_sidebar.save()

        page = project.wikis.get(wiki_path)

        if attachment_path is not None:
            attachement = page.upload(filename = attachment_path.split("/")[-1], filepath=attachment_path)['link']['markdown']

        # page.content = wiki_content
        # if attachment_path is not None:
        #     attachment = 
        #     if replace_content == False:
                
        #     else:
        #         new_content = wiki_content +'\n\n'+ temp['link']['markdown']

        if replace_content == False:
            new_content = page.content + '\n\n'+ wiki_content + '\n\n' + attachement
        else:
            new_content = wiki_content +'\n\n'+ attachement

        page.content = new_content
        page.title = page.slug
        page.save()

        return page.attributes
    
    
    
class GetWiki(Task):
    def __init__(
        self,
        url: str,
        credentials: str = None,
        config_key: str = "GITLAB",
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
            wiki_path: str ="/home",
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
            

        page = project.wikis.get(self.wiki_path)
                      
        # try:
        #     project = gl.projects.get(project_namespace)
        #     page = project.wikis.get(wiki_path)
        #     content = page.attributes['content']
        # except:
        #     GitlabGetError("Wiki page no found")

        return page.attributes['content']
            