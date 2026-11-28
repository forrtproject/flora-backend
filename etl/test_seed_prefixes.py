"""
Offline tests for seed_prefixes.py: keeping the Atlas pipeline's data in
DOI_TABLE across a load. boto3 is replaced by small in-memory fakes, so no
AWS access or credentials are needed.

Run: python -m pytest etl
"""

import bisect
import csv
import importlib
import json
import os
import sys
import types
from decimal import Decimal

import pytest

# The script exits at import without these, and imports boto3 and ftfy.
for _name in ("PREFIX_TABLE", "DOI_TABLE", "SEARCH_TABLE"):
    os.environ.setdefault(_name, f"test-{_name.lower()}")
for _mod, _attrs in (("boto3", {}), ("ftfy", {"fix_text": lambda s: s})):
    try:
        importlib.import_module(_mod)
    except ImportError:
        sys.modules[_mod] = types.SimpleNamespace(**_attrs)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import seed_prefixes as sp  # noqa: E402


# -----------------------------
# Fakes
# -----------------------------

class FakeBatch:
    def __init__(self, table):
        self.table = table

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def put_item(self, Item):
        self.table.writes.append(("put", Item[self.table.key]))
        self.table.items[Item[self.table.key]] = dict(Item)

    def delete_item(self, Key):
        self.table.writes.append(("delete", Key[self.table.key]))
        self.table.items.pop(Key[self.table.key], None)


class FakeTable:
    """Just enough of boto3's Table: paged scan, get_item, batch_writer."""

    def __init__(self, key, items=(), page_size=100, fail_on_scan=None):
        self.key = key
        self.key_schema = [{"AttributeName": key, "KeyType": "HASH"}]
        self.items = {it[key]: dict(it) for it in items}
        self.page_size = page_size
        self.fail_on_scan = fail_on_scan  # 1-based scan call that raises
        self.scan_calls = []
        self.writes = []

    def scan(self, **kwargs):
        self.scan_calls.append(kwargs)
        if self.fail_on_scan == len(self.scan_calls):
            raise RuntimeError("simulated scan failure")
        keys = sorted(self.items)
        start = kwargs.get("ExclusiveStartKey")
        i = bisect.bisect_right(keys, start[self.key]) if start else 0
        page = keys[i:i + self.page_size]

        names = None
        if "ProjectionExpression" in kwargs:
            alias = kwargs.get("ExpressionAttributeNames", {})
            names = [alias.get(n.strip(), n.strip()) for n in kwargs["ProjectionExpression"].split(",")]
        out = []
        for k in page:
            it = self.items[k]
            out.append({n: it[n] for n in names if n in it} if names else dict(it))

        resp = {"Items": out}
        if i + self.page_size < len(keys):
            resp["LastEvaluatedKey"] = {self.key: page[-1]}
        return resp

    def get_item(self, Key, **kwargs):
        it = self.items.get(Key[self.key])
        return {"Item": dict(it)} if it is not None else {}

    def batch_writer(self, **kwargs):
        return FakeBatch(self)


class FakeDynamo:
    def __init__(self, tables):
        self.tables = tables

    def Table(self, name):
        return self.tables[name]


# -----------------------------
# Helpers
# -----------------------------

A = "10.1000/aaa"
B = "10.1000/bbb"
C = "10.1000/ccc"
GONE = "10.1000/gone"

META = {
    "doi": "meta#citation_impact",
    "last_updated": "2026-10-26",
    "n_studies": Decimal("2900"),
}
AGG = {
    "doi": "aggregate#psychology",
    "median": Decimal("4.5"),
    "series": [{"year": Decimal("2020"), "n": Decimal("12")}],
}

TIMELINE = {"last_updated": "2026-10-26", "entries": [{"year": 2020, "n": 3}, {"year": 2021, "n": 5}]}


def blob(**top):
    """An existing record blob, as the ETL and the Atlas pipeline leave it."""
    rec = {"types": ["original"], "title": "Old title", "record": {"replications": [], "stats": {}}}
    rec.update(top)
    return json.dumps(rec)


def write_csv(path, dois):
    """One original-with-replication row per DOI pair (doi_o, doi_r)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["doi_o", "doi_o_hash", "doi_r", "doi_r_hash", "type", "title_o", "title_r"])
        for i, (doi_o, doi_r) in enumerate(dois):
            w.writerow([doi_o, f"a{i}x", doi_r, f"b{i}x" if doi_r else "", "replication", f"New title {i}", "Rep"])
    return str(path)


def run(monkeypatch, tmp_path, doi_items, csv_rows, clear=False, page_size=100, fail_on_scan=None,
        prune="yes", max_fraction=1.0, search_items=(), prefix_fail_on_scan=None):
    """max_fraction defaults to 1.0: these tables are tiny, so one stale item is a big share."""
    tables = {
        "prefix": FakeTable("prefix", [{"prefix": "zzz", "dois": ["10.1000/stale"]}],
                            fail_on_scan=prefix_fail_on_scan),
        "doi": FakeTable("doi", doi_items, page_size=page_size, fail_on_scan=fail_on_scan),
        "search": FakeTable("chunk_id", [{"chunk_id": "meta", "data": "1"}, *search_items]),
    }
    monkeypatch.setattr(sp, "boto3", types.SimpleNamespace(resource=lambda *a, **k: FakeDynamo(tables)))
    monkeypatch.setattr(sp, "PREFIX_TABLE", "prefix")
    monkeypatch.setattr(sp, "DOI_TABLE", "doi")
    monkeypatch.setattr(sp, "SEARCH_TABLE", "search")
    monkeypatch.setattr(sp, "CLEAR_TABLES", clear)
    monkeypatch.setattr(sp, "PRUNE", prune)
    monkeypatch.setattr(sp, "PRUNE_MAX_FRACTION", max_fraction)
    monkeypatch.setattr(sys, "argv", ["seed_prefixes.py", write_csv(tmp_path / "flora.csv", csv_rows)])
    sp.main()
    return tables


def record_of(tables, doi):
    return json.loads(tables["doi"].items[doi]["record"])


def puts(table):
    return [k for op, k in table.writes if op == "put"]


def deletes(table):
    return [k for op, k in table.writes if op == "delete"]


# -----------------------------
# Tests
# -----------------------------

def test_citation_fields_carried_over(monkeypatch, tmp_path, capsys):
    items = [
        {"doi": A, "record": blob(n_citations=8, citation_timeline=TIMELINE)},
        # Top-level item attributes, as boto3 returns them, are accepted too.
        {"doi": B, "record": blob(), "n_citations": Decimal("7"),
         "citation_timeline": {"entries": [{"year": Decimal("2019"), "n": Decimal("7")}]}},
    ]
    tables = run(monkeypatch, tmp_path, items, [(A, B)])

    rec_a = record_of(tables, A)
    assert rec_a["n_citations"] == 8
    assert rec_a["citation_timeline"] == TIMELINE
    assert rec_a["title"] == "New title 0"  # the rest of the record is the new one

    rec_b = record_of(tables, B)
    assert rec_b["n_citations"] == 7
    assert rec_b["citation_timeline"] == {"entries": [{"year": 2019, "n": 7}]}

    assert "Kept citation data for 2 DOIs (0 no longer in the CSV)" in capsys.readouterr().out


def test_old_nested_map_not_carried(monkeypatch, tmp_path):
    old = {"types": ["original"], "record": {"citation_timeline": {"2019": 2}, "n_citations": 2}}
    items = [
        {"doi": A, "record": json.dumps(old)},
        {"doi": B, "record": json.dumps({**old, "n_citations": 9})},
    ]
    tables = run(monkeypatch, tmp_path, items, [(A, B)])

    rec_a = record_of(tables, A)
    assert "citation_timeline" not in rec_a and "n_citations" not in rec_a
    assert "citation_timeline" not in rec_a["record"] and "n_citations" not in rec_a["record"]

    rec_b = record_of(tables, B)
    assert rec_b["n_citations"] == 9
    assert "citation_timeline" not in rec_b
    assert "citation_timeline" not in rec_b["record"] and "n_citations" not in rec_b["record"]


def test_pagination(monkeypatch, tmp_path):
    items = [{"doi": d, "record": blob(n_citations=n)} for n, d in enumerate([A, B, C], start=1)]
    tables = run(monkeypatch, tmp_path, items, [(A, B), (C, "")], page_size=1)

    calls = tables["doi"].scan_calls
    assert len(calls) == 3
    assert "ExclusiveStartKey" not in calls[0]
    assert [c["ExclusiveStartKey"]["doi"] for c in calls[1:]] == [A, B]
    assert all("#rec" in c["ProjectionExpression"] for c in calls)
    assert calls[0]["ExpressionAttributeNames"]["#rec"] == "record"
    assert [record_of(tables, d)["n_citations"] for d in (A, B, C)] == [1, 2, 3]


def test_bad_json_item_skipped(monkeypatch, tmp_path, capsys):
    items = [
        {"doi": A, "record": "{not json"},
        {"doi": B, "record": blob(n_citations=4)},
        {"doi": C, "record": "[1, 2]"},  # valid JSON, but not a record
    ]
    tables = run(monkeypatch, tmp_path, items, [(A, B), (C, "")])

    assert "n_citations" not in record_of(tables, A)
    assert record_of(tables, B)["n_citations"] == 4
    assert "n_citations" not in record_of(tables, C)
    out, err = capsys.readouterr()
    assert "2 items in doi have an unreadable record" in err
    assert "Kept citation data for 1 DOIs (0 no longer in the CSV)" in out
    assert out.rstrip().endswith("Done.")


@pytest.mark.parametrize("clear", [False, True])
def test_scan_failure_writes_nothing(monkeypatch, tmp_path, capsys, clear):
    items = [{"doi": d, "record": blob(n_citations=1)} for d in (A, B, C)] + [META]
    with pytest.raises(SystemExit) as exc:
        run(monkeypatch, tmp_path, items, [(A, B)], clear=clear, page_size=2, fail_on_scan=2)
    assert exc.value.code not in (0, None)
    assert "nothing was written" in capsys.readouterr().err

    # run() raised before returning them, so reach the tables through the fake boto3.
    tables = sp.boto3.resource().tables
    assert all(t.writes == [] for t in tables.values())
    assert json.loads(tables["doi"].items[A]["record"])["n_citations"] == 1


def test_clearing_restores_meta_and_aggregate(monkeypatch, tmp_path, capsys):
    items = [
        META,
        AGG,
        {"doi": A, "record": blob(n_citations=8, citation_timeline=TIMELINE)},
        {"doi": GONE, "record": blob(n_citations=3)},
    ]
    tables = run(monkeypatch, tmp_path, items, [(A, B)], clear=True)
    doi_items = tables["doi"].items

    assert doi_items["meta#citation_impact"] == META
    assert doi_items["aggregate#psychology"] == AGG
    assert GONE not in doi_items
    assert sorted(doi_items) == sorted([A, B, META["doi"], AGG["doi"]])
    assert record_of(tables, A)["citation_timeline"] == TIMELINE
    assert "zzz" not in tables["prefix"].items

    out = capsys.readouterr().out
    assert "Kept citation data for 1 DOIs (1 no longer in the CSV)" in out
    assert "Kept 2 meta/aggregate items in doi (put back after clearing)" in out
    # The clear already removed GONE; there is nothing left to prune.
    assert "Stale:" not in out and "Pruned" not in out


def test_doi_no_longer_in_csv_not_written(monkeypatch, tmp_path, capsys):
    gone = {"doi": GONE, "record": blob(n_citations=3)}
    items = [META, AGG, gone, {"doi": A, "record": blob(n_citations=8)}]
    tables = run(monkeypatch, tmp_path, items, [(A, B)], prune="no")

    written = puts(tables["doi"])
    assert GONE not in written
    assert tables["doi"].items[GONE] == gone
    # Non-clearing runs never touch the Atlas items either.
    assert not any(k.startswith(sp.ATLAS_ITEM_PREFIXES) for _, k in tables["doi"].writes)
    assert tables["doi"].items[META["doi"]] == META
    assert sorted(written) == [A, B]
    out = capsys.readouterr().out
    assert "Kept citation data for 1 DOIs (1 no longer in the CSV)" in out
    assert "PRUNE=no" in out
    assert all(deletes(t) == [] for t in tables.values())


def test_csv_value_with_atlas_key_is_never_written(monkeypatch, tmp_path):
    tables = run(monkeypatch, tmp_path, [META], [(A, "meta#citation_impact")])

    assert tables["doi"].items[META["doi"]] == META
    assert puts(tables["doi"]) == [A]
    assert deletes(tables["doi"]) == []
    assert all("meta#citation_impact" not in it["dois"] for it in tables["prefix"].items.values())


# -----------------------------
# Pruning
# -----------------------------

def stale_setup():
    """GONE left the CSV; the prefix "zzz" and search chunk "7" are left from an older load."""
    items = [META, AGG, {"doi": GONE, "record": blob(n_citations=3)},
             {"doi": A, "record": blob(n_citations=8)}]
    search = [{"chunk_id": "7", "data": "[]"}, {"chunk_id": "notes", "data": "x"}]
    return items, search


def test_prune_removes_what_left_the_csv(monkeypatch, tmp_path, capsys):
    items, search = stale_setup()
    tables = run(monkeypatch, tmp_path, items, [(A, B)], search_items=search)
    doi = tables["doi"]

    assert sorted(doi.items) == sorted([A, B, META["doi"], AGG["doi"]])
    assert doi.items[META["doi"]] == META and doi.items[AGG["doi"]] == AGG
    assert not any(k.startswith(sp.ATLAS_ITEM_PREFIXES) for _, k in doi.writes)
    assert record_of(tables, A)["n_citations"] == 8

    # Deleted only after every current record is written.
    assert deletes(doi) == [GONE]
    assert doi.writes.index(("delete", GONE)) > max(doi.writes.index(("put", d)) for d in (A, B))

    assert "zzz" not in tables["prefix"].items and len(tables["prefix"].items) == 2
    # Chunk 0 is this load's, "meta" holds the count, and a non-numeric key is not ours to judge.
    assert sorted(tables["search"].items) == ["0", "meta", "notes"]

    out = capsys.readouterr().out
    assert "Stale: 1 of 2 DOI items (50.0%), 1 prefixes, 1 search chunks" in out
    assert f"  stale DOI: {GONE}" in out
    assert "Pruned 1 DOI items, 1 prefixes and 1 search chunks no longer in the CSV" in out
    assert out.rstrip().endswith("Done.")


def test_prune_refused_over_the_limit(monkeypatch, tmp_path, capsys):
    items, search = stale_setup()
    with pytest.raises(SystemExit) as exc:
        run(monkeypatch, tmp_path, items, [(A, B)], max_fraction=0.25, search_items=search)
    assert exc.value.code == 5

    tables = sp.boto3.resource().tables
    assert all(deletes(t) == [] for t in tables.values())
    assert GONE in tables["doi"].items and "zzz" in tables["prefix"].items and "7" in tables["search"].items
    # The load itself went through.
    assert B in tables["doi"].items and record_of(tables, A)["title"] == "New title 0"

    out = capsys.readouterr().out
    assert "Prune refused: 50.0% of the DOI table is more than PRUNE_MAX_FRACTION=25%" in out
    assert "Done." not in out


def test_prune_at_the_limit_goes_ahead(monkeypatch, tmp_path):
    items, _ = stale_setup()
    tables = run(monkeypatch, tmp_path, items, [(A, B)], max_fraction=0.5)
    assert deletes(tables["doi"]) == [GONE]


def test_prune_force_ignores_the_limit(monkeypatch, tmp_path):
    items, _ = stale_setup()
    tables = run(monkeypatch, tmp_path, items, [(A, B)], prune="force", max_fraction=0.25)
    assert deletes(tables["doi"]) == [GONE]
    assert GONE not in tables["doi"].items


def test_prune_dry_run_deletes_nothing(monkeypatch, tmp_path, capsys):
    items, search = stale_setup()
    tables = run(monkeypatch, tmp_path, items, [(A, B)], prune="dry-run", max_fraction=0.25,
                 search_items=search)

    assert all(deletes(t) == [] for t in tables.values())
    assert GONE in tables["doi"].items
    out = capsys.readouterr().out
    assert f"  stale DOI: {GONE}" in out
    assert "nothing was deleted" in out and "Pruned" not in out
    assert out.rstrip().endswith("Done.")


def test_nothing_stale_deletes_nothing(monkeypatch, tmp_path, capsys):
    tables = run(monkeypatch, tmp_path, [{"doi": A, "record": blob()}], [(A, B)], max_fraction=0.25)
    assert deletes(tables["doi"]) == [] and deletes(tables["search"]) == []
    assert "Pruned 0 DOI items" in capsys.readouterr().out


def test_prune_keeps_a_doi_that_is_only_a_replication(monkeypatch, tmp_path):
    items = [{"doi": C, "record": blob()}, {"doi": GONE, "record": blob()}, {"doi": A, "record": blob()}]
    tables = run(monkeypatch, tmp_path, items, [("", C), (A, B)])
    assert deletes(tables["doi"]) == [GONE]
    assert record_of(tables, C)["types"] == ["replication"]


def test_prune_failure_after_the_load(monkeypatch, tmp_path, capsys):
    items, _ = stale_setup()
    with pytest.raises(SystemExit) as exc:
        run(monkeypatch, tmp_path, items, [(A, B)], prefix_fail_on_scan=1)
    assert exc.value.code == 6

    tables = sp.boto3.resource().tables
    assert B in tables["doi"].items and GONE in tables["doi"].items
    assert all(deletes(t) == [] for t in tables.values())
    out = capsys.readouterr().out
    assert "Prune failed: RuntimeError('simulated scan failure')" in out
    assert "Done." not in out


def reload_with(monkeypatch, **env):
    for name in ("PRUNE", "PRUNE_MAX_FRACTION"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    importlib.reload(sp)


@pytest.fixture
def restore_module(monkeypatch):
    yield
    # Leave the module as the other tests expect it, whatever the test did.
    for name in ("PRUNE", "PRUNE_MAX_FRACTION"):
        monkeypatch.delenv(name, raising=False)
    importlib.reload(sp)


def test_prune_defaults(monkeypatch, restore_module):
    # The workflow relies on these when it sets nothing.
    reload_with(monkeypatch)
    assert sp.PRUNE == "yes" and sp.PRUNE_MAX_FRACTION == 0.25


@pytest.mark.parametrize("env", [
    {"PRUNE": "maybe"},
    {"PRUNE_MAX_FRACTION": "0"},
    {"PRUNE_MAX_FRACTION": "1.5"},
    {"PRUNE_MAX_FRACTION": "nan"},
    {"PRUNE_MAX_FRACTION": "a quarter"},
])
def test_bad_prune_settings_stop_at_start(monkeypatch, restore_module, capsys, env):
    with pytest.raises(SystemExit) as exc:
        reload_with(monkeypatch, **env)
    assert exc.value.code == 1
    assert "ERROR: PRUNE" in capsys.readouterr().err
