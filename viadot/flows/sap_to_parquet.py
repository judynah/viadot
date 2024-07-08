from prefect import Flow
from typing import  Any, Dict, List, Literal
from viadot.tasks import SAPRFCToDF
from viadot.task_utils import df_to_parquet

class SAPtoParquet(Flow):
    def __init__(
        self,
        name, 
        query: str,
        parquet_file_path: str,
        sap_config_key: str = None,
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = "fail",
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
            sap_config_key (str, optional): The key inside local config containing the credentials. Defaults to None.
            if_exists (Literal, optional):  What to do if the file already exists. Defaults to "fail".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

        self.query = query
        self.sap_config_key = sap_config_key
        self.timeout = timeout

        self.parquet_file_path = parquet_file_path
        self.if_exists = if_exists

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_glow(self) -> Flow:
        df_task = SAPRFCToDF(timeout=self.timeout)

        df = df_task.bind(
            sap_credentials_key=self.sap_config_key, query=self.query, flow=self
        )

        parquet = df_to_parquet(
            df = df, path=self.parquet_file_path, if_exists=self.if_exists, flow=self
        )

        