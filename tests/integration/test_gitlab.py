
from viadot.sources.gitlab import Gitlab
import pytest
from viadot.config import local_config
from viadot.exceptions import CredentialError, ValidationError
from unittest import mock

URL = "https://gitlab.com/explore/projects"


def test_create_gitlab_instance():
    gl = Gitlab(url=URL)
    assert gl

def test_connection_fail():
    test_creds = {
        "username" : "user",
        "token" : "token",
    }
    with pytest.raises(CredentialError):
        gl = Gitlab(credentials=test_creds)

def test_connection_gitlab():
    with mock.patch("viadot.sources.gitlab.Gitlab.get_conn") as mock_method:
        mock_method.return_value = Gitlab

        gl = Gitlab(url=URL, credentials=None)
        result = gl.get_conn()
        assert type(result) == Gitlab



        


