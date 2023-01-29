import os
from viadot.sources.iceberg import LivyClient, LivySession, SESSION_STATE_NOT_READY
import uuid

LIVY_URL = os.environ["LIVY_URL"]
LIVY_CREDENTIALS = {"username": os.environ["LIVY_USERNAME"], "password": os.environ["LIVY_PASSWORD"]}

def test_livy_client():
    client = LivyClient(url=LIVY_URL, credentials=LIVY_CREDENTIALS)
    assert type(client.server_version() == str)

def test_livy_session():
    session = LivySession(url=LIVY_URL, credentials=LIVY_CREDENTIALS)
    assert session.state not in SESSION_STATE_NOT_READY
    session.close()

def test_livy_session_run():
    # TODO
    pass

def test_pandas_df_to_spark_df():
    # TODO
    pass

def test__get_fqn():
    # TODO 
    pass

def test_to_iceberg():
    # TODO
    pass