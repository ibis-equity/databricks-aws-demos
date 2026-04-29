import json
from pathlib import Path

from tests.helpers.notebook_loader import load_functions_from_notebook


ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "_resources" / "01-setup.py"


class _FakeNow:
    def isoformat(self):
        return "2026-01-01T00:00:00"


class _FakeDateTime:
    class datetime:
        @staticmethod
        def now():
            return _FakeNow()


class _FakeRandom:
    @staticmethod
    def choice(values):
        return values[0]

    @staticmethod
    def random():
        return 0.42


class _KinesisRecorder:
    def __init__(self):
        self.records = []

    def put_record(self, **kwargs):
        self.records.append(kwargs)


def test_get_data_returns_expected_shape():
    funcs, _ = load_functions_from_notebook(
        str(NOTEBOOK),
        ["get_data"],
        injected_globals={"datetime": _FakeDateTime, "random": _FakeRandom},
    )

    data = funcs["get_data"]()

    assert set(data.keys()) == {"event_time", "ticker", "price"}
    assert data["event_time"] == "2026-01-01T00:00:00"
    assert data["ticker"] == "AAPL"
    assert data["price"] == 42.0


def test_generate_writes_300_records_to_kinesis_client():
    funcs, _ = load_functions_from_notebook(
        str(NOTEBOOK),
        ["get_data", "generate"],
        injected_globals={"datetime": _FakeDateTime, "random": _FakeRandom, "json": json},
    )

    client = _KinesisRecorder()
    funcs["generate"]("demo-stream", client)

    assert len(client.records) == 300
    first = client.records[0]
    assert first["StreamName"] == "demo-stream"
    assert first["PartitionKey"] == "partitionkey"
    assert json.loads(first["Data"])["ticker"] == "AAPL"

