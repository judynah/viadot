from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, Iterator

import requests

from .base import Source


# Possible session states are defined here:
# https://github.com/apache/incubator-livy/blob/master/core/src/main/scala/
# org/apache/livy/sessions/SessionState.scala
class SessionState(Enum):
    NOT_STARTED = "not_started"
    STARTING = "starting"
    RECOVERING = "recovering"
    IDLE = "idle"
    RUNNING = "running"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"
    KILLED = "killed"
    SUCCESS = "success"


SESSION_STATE_NOT_READY = {SessionState.NOT_STARTED, SessionState.STARTING}
SESSION_STATE_FINISHED = {
    SessionState.ERROR,
    SessionState.DEAD,
    SessionState.KILLED,
    SessionState.SUCCESS,
}


@dataclass
class Session:
    id: int
    state: SessionState

    @classmethod
    def from_json(cls, data: dict) -> "Session":
        return cls(
            data["id"],
            SessionState(data["state"]),
        )



def polling_intervals(
    start: Iterable[float], rest: float, max_duration: float = None
) -> Iterator[float]:
    def _intervals():
        yield from start
        while True:
            yield rest

    cumulative = 0.0
    for interval in _intervals():
        cumulative += interval
        if max_duration is not None and cumulative > max_duration:
            break
        yield interval


class LivyClient:
    """A client for sending requests to a Livy server.

    :param url: The URL of the Livy server.
    :param credentials: Credentials to pass to requests.Session `auth` parameter.
    :param verify: Either a boolean, in which case it controls whether we
        verify the server’s TLS certificate, or a string, in which case it must
        be a path to a CA bundle to use. Defaults to ``True``.
    :param requests_session: A specific ``requests.Session`` to use, allowing
        advanced customisation. The caller is responsible for closing the
        session.
    """

    def __init__(
        self,
        url: str,
        credentials: dict = None,
        verify: bool = True,
        requests_session: requests.Session = None,
    ) -> None:

        self.url = url
        self.credentials = credentials
        self.verify = verify

        if requests_session is None:
            self.session = requests.Session()
            self.managed_session = True
        else:
            self.session = requests_session
            self.managed_session = False

    def close(self) -> None:
        """Close the underlying requests session, if managed by this class."""
        if self.managed_session:
            self.session.close()

    def get(self, endpoint: str = "", params: dict = None) -> dict:
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, data: dict = None) -> dict:
        return self._request("POST", endpoint, data)

    def delete(self, endpoint: str = "") -> dict:
        return self._request("DELETE", endpoint)

    def _request(
        self,
        method: str,
        endpoint: str,
        data: dict = None,
        params: dict = None,
    ) -> dict:
        url = self.url.rstrip("/") + endpoint
        response = self.session.request(
            method,
            url,
            auth=(self.credentials["username"], self.credentials["password"]),
            verify=self.verify,
            json=data,
            params=params,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _session_url(session_id):
        return "/sessions/{}".format(session_id)

    @staticmethod
    def _statements_url(session_id):
        return "/sessions/{}/statements".format(session_id)

    @staticmethod
    def _statement_url(session_id, statement_id):
        return "/sessions/{}/statements/{}".format(session_id, statement_id)

    def server_version(self) -> str:
        """Get the version of Livy running on the server."""
        return self.get("/version")["version"]

    def create_session(
        self,
        name: str,
        jars: list[str] = None,
        conf: Dict[str, Any] = None,
        py_files: list[str] = None,
        files: list[str] = None,
        num_executors: int = None
    ) -> Session:
        payload = {
            "name": name,
            "jars": jars,
            "conf": conf,
            "pyFiles": py_files,
            "files": files,
            "numExecutors": num_executors
        }
        response = self._client.post("/sessions", data=payload)
        return Session.from_json(response)

    def get_session(self, id):
        response = self.get(self._session_url(id))
        return Session.from_json(response)



class LivySession:
    def __init__(self, url: str, id: int, credentials: dict = None):
        """A Livy session, trying to mimic a Spark session.

        This works by creating a Livy session using LivyClient,
        and then utilizing it for further operations.

        Since Live API is asynchronous, we create the session in two steps:
        1) sending a POST request to the /sessions endpoint
        2) polling the sessions/{session_id} endpoint until the session
        is established

        To accomplish 1), we use LivyClient.create_session(). Then, we
        use the wait() method to await for the correct state.

        Args:
            url (str): The URL of the Livy server.
            credentials (dict, optional): Livy credentials to pass to 
                requests.Session `auth` parameter. Defaults to None.
        """
        self.client = LivyClient(url=url, credentials=credentials)
        self.id = self.client.create_session().id
        self.wait()

    def wait(self) -> None:
        """Wait for the session to be ready."""
        intervals = polling_intervals([0.1, 0.2, 0.3, 0.5], 1.0)
        while self.state in SESSION_STATE_NOT_READY:
            time.sleep(next(intervals))
        else:
            self.id = self.state.id

    @property
    def state(self) -> SessionState:
        """The state of the managed Spark session."""
        session = self.client.get_session(self.id)
        if session is None:
            raise ValueError("Session not found. Maybe it has been shut down?")
        return session.state

    def close(self) -> None:
        self.client.delete(self.client._session_url(self.id))
        self.client.close()

    def run(self, code: str):
        """Run a pyspark command and retrieve the result as a pandas DataFrame.

        Args:
            code (str): The code to execute remotely on the Spark cluster.

        Returns:
            df (pd.DataFrame): A pandas DataFrame containing the resulting data.
        """
        # TODO
        pass


class SparkDataFrame:

    def __init__(self, session: LivySession) -> None:
        self.session = session

    def _get_fqn(table, schema: str = None, database: str = None) -> str:
        if all([database, schema, table]):
            fqn = f"{database}.{schema}.{table}"
        elif schema and table:
            fqn = f"{schema}.{table}"
        elif table:
            fqn = table
        return fqn

    def to_iceberg(
        self, table, schema: str = None, database: str = None, if_exists: str = "fail"
    ) -> None:
        """Write a Spark DataFrame to an Iceberg table.

        Args:
            table (_type_): The name of the table to which to write.
            schema (str, optional): The schema where the table is located. 
                Defaults to, which means the default schema will be used.
            database (str, optional): The database where the schema is located.
                Defaults to None, which means the default catalog will be used.
            if_exists (str, optional): What to do if the table already exists.
                Defaults to "fail".
        """
        fqn = self._get_fqn(table=table, schema=schema, database=database)

        self.session.run(f"df.writeTo({fqn}, if_exists={if_exists})")





class Iceberg(Source):
    def __init__(self, credentials: dict = None, *args, **kwargs):
        
        super().__init__(*args, credentials=credentials, **kwargs)
        
        self._session = None

#     @property
#     def session(self) -> LivySession:
#         if self._session is None:
#             session = self._create_spark_session()
#             self._session = session
#             return session
#         return self._session


#     def _create_spark_session(self) -> LivySession:
        
#         else:

