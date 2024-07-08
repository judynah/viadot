from prefect import Flow
from typing import  Any, Dict, List, Literal
from viadot.tasks import SAPRFCToDF
from viadot.task_utils import df_to_parquet

class SAPtoParquet(Flow):
    def __init__(
        self,
        name: str,
        query: str,
        parquet_file_path: str,
        sap_credentials: dict = None,
        sap_credentials_key: str = "SAP",
        rfc_total_col_width_character_limit: int = 400,
        sep: str = None,
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = "fail",
        func: str = "RFC_READ_TABLE",
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for uploading data from SAP database using the RFC protocol to Parquet file.

        Args:
            name (str, required): The name of the flow.
            query (str, required): The query to execute on the SAP database. If don't start with "SELECT"
                returns empty DataFrame.
            parquet_file_path (str, required): Path to output parquet file.
            sap_credentials (dict, optional): The credentials to use to authenticate with SAP. Defaults to None..
            sap_credentials_key (str, optional): The key inside local config containing the credentials. Defaults to None.
            rfc_total_col_width_character_limit (str, optional): Number of characters by which query will be split in
            chunks in case of too many columns for RFC function. According to SAP documentation, the
            limit is 512 characters. However, we observed SAP raising an exception even on a slightly
            lower number of characters, so we add a safety margin. Defaults to 400.
            sep (str, optional): The separator to use when reading query results. If not provided,
            multiple options are automatically tried. Defaults to None.
            if_exists (Literal, optional):  What to do if the file already exists. Defaults to "fail".
            func (str, optional): SAP RFC function to use. Defaults to "RFC_READ_TABLE".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

         # SAPRFCToDF
        self.query = query
        self.sap_credentials = sap_credentials
        self.sap_credentials_key = sap_credentials_key
        self.timeout = timeout
        self.func = func
        self.rfc_total_col_width_character_limit = rfc_total_col_width_character_limit
        self.sep = sep

         # df_to_parquet
        self.parquet_file_path = parquet_file_path
        self.if_exists = if_exists

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:
        df_task = SAPRFCToDF(
            timeout=self.timeout, 
            credentials = self.sap_credentials,
            sap_credentials_key = self.sap_credentials_key
        )

        df = df_task.bind(
            query=self.query, 
            flow=self,
            func=self.func,
            rfc_total_col_width_character_limit = self.rfc_total_col_width_character_limit,
            sep=self.sep
        )
        
        parquet = df_to_parquet(
            df = df, 
            path=self.parquet_file_path, 
            if_exists=self.if_exists, 
            flow=self
        )