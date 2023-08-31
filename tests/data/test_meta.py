import pandas as pd
import pytest

from ixmp4.core.exceptions import SchemaError
from ixmp4.data.abstract.meta import RunMetaEntry

from ..utils import all_platforms, assert_unordered_equality

TEST_ENTRIES = [
    ("Boolean", True, RunMetaEntry.Type.BOOL),
    ("Float", 0.2, RunMetaEntry.Type.FLOAT),
    ("Integer", 1, RunMetaEntry.Type.INT),
    ("String", "Value", RunMetaEntry.Type.STR),
]

TEST_ENTRIES_DF = pd.DataFrame(
    [[id, key, type, value] for id, (key, value, type) in enumerate(TEST_ENTRIES, 1)],
    columns=["id", "key", "type", "value"],
)


@all_platforms
class TestDataMeta:
    def test_create_get_entry(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")

        for key, value, type in TEST_ENTRIES:
            entry = test_mp.backend.meta.create(run.id, key, value)
            assert entry.key == key
            assert entry.value == value
            assert entry.type == type

        for key, value, type in TEST_ENTRIES:
            entry = test_mp.backend.meta.get(run.id, key)
            assert entry.key == key
            assert entry.value == value
            assert entry.type == type

    def test_entry_unique(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        test_mp.backend.meta.create(run.id, "Key", "Value")

        with pytest.raises(RunMetaEntry.NotUnique):
            test_mp.backend.meta.create(run.id, "Key", "Value")

        with pytest.raises(RunMetaEntry.NotUnique):
            test_mp.backend.meta.create(run.id, "Key", 1)

    def test_entry_not_found(self, test_mp):
        with pytest.raises(RunMetaEntry.NotFound):
            test_mp.backend.meta.get(-1, "Key")

        run = test_mp.Run("Model", "Scenario", "new")

        with pytest.raises(RunMetaEntry.NotFound):
            test_mp.backend.meta.get(run.id, "Key")

    def test_delete_entry(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        entry = test_mp.backend.meta.create(run.id, "Key", "Value")
        test_mp.backend.meta.delete(entry.id)

        with pytest.raises(RunMetaEntry.NotFound):
            test_mp.backend.meta.get(run.id, "Key")

    def test_list_entry(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")

        for key, value, _ in TEST_ENTRIES:
            entry = test_mp.backend.meta.create(run.id, key, value)

        entries = test_mp.backend.regions.list()

        for (key, value, type), entry in zip(TEST_ENTRIES, entries):
            assert entry.key == key
            assert entry.value == value
            assert entry.type == type

    def test_tabulate_entry(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")

        for key, value, _ in TEST_ENTRIES:
            test_mp.backend.meta.create(run.id, key, value)

        true_entries = TEST_ENTRIES_DF.copy()
        true_entries["run__id"] = run.id

        entries = test_mp.backend.meta.tabulate()
        assert_unordered_equality(entries, true_entries)

    def test_entry_bulk_operations(self, test_mp):
        run = test_mp.Run("Model", "Scenario", version="new")
        entries = TEST_ENTRIES_DF.copy()
        entries["run__id"] = run.id

        # == Full Addition ==
        test_mp.backend.meta.bulk_upsert(entries.drop(columns=["id", "type"]))
        ret = test_mp.backend.meta.tabulate()
        assert_unordered_equality(entries, ret)

        # == Partial Removal ==
        # Remove half the data
        remove_data = entries.head(len(entries) // 2).drop(
            columns=["value", "id", "type"]
        )
        remaining_data = entries.tail(len(entries) // 2).reset_index(drop=True)
        test_mp.backend.meta.bulk_delete(remove_data)

        ret = test_mp.backend.meta.tabulate()
        assert_unordered_equality(remaining_data, ret)

        # == Partial Update / Partial Addition ==
        entries["value"] = -9.9
        entries["id"] = [5, 6, 3, 4]

        updated_entries = pd.DataFrame(
            [
                [5, "Boolean", -9.9, RunMetaEntry.Type.FLOAT],
                [6, "Float", -9.9, RunMetaEntry.Type.FLOAT],
                [3, "Integer", -9.9, RunMetaEntry.Type.FLOAT],
                [4, "String", -9.9, RunMetaEntry.Type.FLOAT],
            ],
            columns=["id", "key", "value", "type"],
        )
        updated_entries["run__id"] = run.id
        updated_entries["value"] = updated_entries["value"].astype("object")  # type: ignore

        test_mp.backend.meta.bulk_upsert(updated_entries.drop(columns=["id", "type"]))
        ret = test_mp.backend.meta.tabulate()

        assert_unordered_equality(updated_entries, ret, check_like=True)

        # == Full Removal ==
        remove_data = entries.drop(columns=["value", "id", "type"])
        test_mp.backend.meta.bulk_delete(remove_data)

        ret = test_mp.backend.meta.tabulate()
        assert ret.empty

    def test_meta_bulk_exceptions(self, test_mp):
        entries = pd.DataFrame(
            [
                ["Boolean", -9.9],
                ["Float", -9.9],
                ["Integer", -9.9],
                ["String", -9.9],
            ],
            columns=["key", "value"],
        )
        run = test_mp.Run("Model", "Scenario", version="new")
        entries["run__id"] = run.id

        duplicated_entries = pd.concat([entries] * 2, ignore_index=True)

        with pytest.raises(RunMetaEntry.NotUnique):
            test_mp.backend.meta.bulk_upsert(duplicated_entries)

        entries["foo"] = "bar"

        with pytest.raises(SchemaError):
            test_mp.backend.meta.bulk_upsert(entries)

        entries = entries.drop(columns=["value"])
        with pytest.raises(SchemaError):
            test_mp.backend.meta.bulk_delete(entries)
