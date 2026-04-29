import json
from pathlib import Path

from tests.helpers.notebook_loader import load_functions_from_notebook


ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "_resources" / "01-config.py"


class _FakeClientError(Exception):
    pass


class _SecretsClient:
    def __init__(self, payload):
        self._payload = payload

    def get_secret_value(self, SecretId):
        return {"SecretString": json.dumps({"password": self._payload})}


class _Session:
    def __init__(self, payload):
        self._payload = payload

    def client(self, service_name, region_name):
        assert service_name == "secretsmanager"
        assert region_name == "ap-southeast-2"
        return _SecretsClient(self._payload)


class _Boto3Module:
    class session:
        Session = None


class _Scope:
    def __init__(self, name):
        self.name = name


class _SecretService:
    def __init__(self, scopes=None):
        self._scopes = scopes or []
        self.created = False

    def list_scopes(self):
        return self._scopes

    def create_scope(self, scope):
        self.created = True

    def put_secret(self, **kwargs):
        pass

    def put_acl(self, **kwargs):
        pass


class _WorkspaceClient:
    def __init__(self, secrets):
        self.secrets = secrets


def test_get_secret_returns_password_value():
    fake_boto3 = _Boto3Module()
    fake_boto3.session.Session = lambda: _Session("pw-123")

    funcs, _ = load_functions_from_notebook(
        str(NOTEBOOK),
        ["get_secret"],
        injected_globals={"boto3": fake_boto3, "ClientError": _FakeClientError, "json": json},
    )

    assert funcs["get_secret"]("ap-southeast-2", "my-secret") == "pw-123"


def test_check_secret_creates_scope_when_missing():
    secrets = _SecretService(scopes=[_Scope("other")])
    workspace_client = _WorkspaceClient(secrets)

    funcs, _ = load_functions_from_notebook(
        str(NOTEBOOK),
        ["create_secret", "check_secret"],
        injected_globals={
            "w": workspace_client,
            "workspace": type("ws", (), {"AclPermission": type("acl", (), {"MANAGE": "MANAGE"})}),
            "spark": type("s", (), {"conf": type("c", (), {"get": staticmethod(lambda _: "pw")})}),
        },
    )

    funcs["check_secret"]()
    assert secrets.created is True


def test_check_secret_noops_when_scope_exists():
    secrets = _SecretService(scopes=[_Scope("q_fed")])
    workspace_client = _WorkspaceClient(secrets)

    funcs, _ = load_functions_from_notebook(
        str(NOTEBOOK),
        ["create_secret", "check_secret"],
        injected_globals={
            "w": workspace_client,
            "workspace": type("ws", (), {"AclPermission": type("acl", (), {"MANAGE": "MANAGE"})}),
            "spark": type("s", (), {"conf": type("c", (), {"get": staticmethod(lambda _: "pw")})}),
        },
    )

    funcs["check_secret"]()
    assert secrets.created is False

