from pathlib import Path

from tests.helpers.notebook_loader import load_functions_from_notebook


ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "_resources" / "00-basedata.py"


class _FsRecorder:
    def __init__(self):
        self.cp_calls = []
        self.rm_calls = []

    def cp(self, src, dst):
        self.cp_calls.append((src, dst))

    def rm(self, path, recursive):
        self.rm_calls.append((path, recursive))


class _Dbutils:
    def __init__(self, fs):
        self.fs = fs


class _SparkRecorder:
    def __init__(self):
        self.sql_calls = []

    def sql(self, query):
        self.sql_calls.append(query)


def test_move_file_copies_and_increments_counter():
    fs = _FsRecorder()
    funcs, _ = load_functions_from_notebook(
        str(NOTEBOOK),
        ["move_file"],
        injected_globals={"dbutils": _Dbutils(fs), "cloud_storage_path": "s3://bucket"},
    )

    new_count = funcs["move_file"](7)

    assert new_count == 8
    assert fs.cp_calls == [
        (
            "/databricks-datasets/iot-stream/data-device/part-00007.json.gz",
            "s3://bucket/ingest/part-00007.json.gz",
        )
    ]


def test_add_data_loads_three_files():
    fs = _FsRecorder()
    funcs, _ = load_functions_from_notebook(
        str(NOTEBOOK),
        ["move_file", "add_data"],
        injected_globals={"dbutils": _Dbutils(fs), "cloud_storage_path": "s3://bucket"},
    )

    final_count = funcs["add_data"](1)

    assert final_count == 4
    assert len(fs.cp_calls) == 3


def test_clean_up_drops_db_and_resets_folders():
    fs = _FsRecorder()
    spark = _SparkRecorder()
    funcs, ns = load_functions_from_notebook(
        str(NOTEBOOK),
        ["clean_up", "add_data"],
        injected_globals={
            "dbutils": _Dbutils(fs),
            "spark": spark,
            "cloud_storage_path": "s3://bucket",
            "dbName": "demo_db",
        },
    )

    called = {"count": 0}

    def _fake_add_data(x):
        called["count"] += 1
        return x + 3

    ns["add_data"] = _fake_add_data

    funcs["clean_up"]()

    assert spark.sql_calls == ["DROP DATABASE IF EXISTS `demo_db` CASCADE"]
    assert fs.rm_calls == [
        ("s3://bucket/ingest/", True),
        ("s3://bucket/ingest_sns/", True),
    ]
    assert called["count"] == 1

