import numpy as np
import pandas as pd
import pytest

import ixmp4
from ixmp4.core.exceptions import InvalidRunMeta, SchemaError
from ixmp4.data.abstract.meta import RunMetaEntry
from ixmp4.data.backend import SqlAlchemyBackend
from ixmp4.data.db.versions import Operation

from .. import utils

TEST_ENTRIES: list[tuple[str, bool | float | int | str, str]] = [
    ("Boolean", True, RunMetaEntry.Type.BOOL),
    ("Float", 0.2, RunMetaEntry.Type.FLOAT),
    ("Integer", 1, RunMetaEntry.Type.INT),
    ("String", "Value", RunMetaEntry.Type.STR),
]

TEST_ENTRIES_DF = pd.DataFrame(
    [[id, key, type, value] for id, (key, value, type) in enumerate(TEST_ENTRIES, 1)],
    columns=["id", "key", "dtype", "value"],
)

TEST_ILLEGAL_META_KEYS = {"model", "scenario", "id", "version", "is_default"}


class TestDataMeta:
    def test_create_get_entry(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        run.set_as_default()

        creations = []
        transaction_id = 5  # five transactions already occured above
        for key, value, type in TEST_ENTRIES:
            transaction_id += 1
            entry = platform.backend.meta.create(run.id, key, value)

            assert entry.key == key
            assert entry.value == value
            assert entry.dtype == type

            creations.append(
                [
                    entry.id,
                    entry.key,
                    entry.dtype,
                    entry.run__id,
                    entry.value_int,
                    entry.value_str,
                    entry.value_float,
                    entry.value_bool,
                    transaction_id,
                    None,
                    Operation.INSERT,
                ]
            )

        for key, value, type in TEST_ENTRIES:
            entry = platform.backend.meta.get(run.id, key)
            assert entry.key == key
            assert entry.value == value
            assert entry.dtype == type

        @utils.versioning_test(platform.backend)
        def assert_versions(backend: SqlAlchemyBackend) -> None:
            expected_versions = pd.DataFrame(
                creations,
                columns=[
                    "id",
                    "key",
                    "dtype",
                    "run__id",
                    "value_int",
                    "value_str",
                    "value_float",
                    "value_bool",
                    "transaction_id",
                    "end_transaction_id",
                    "operation_type",
                ],
            ).replace({np.nan: None})
            vdf = backend.meta.versions.tabulate()
            utils.assert_unordered_equality(expected_versions, vdf, check_dtype=False)

    def test_illegal_key(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        for key in TEST_ILLEGAL_META_KEYS:
            with pytest.raises(InvalidRunMeta, match="Illegal meta key: " + key):
                platform.backend.meta.create(run.id, key, "foo")

            df = pd.DataFrame(
                {"run__id": [run.id] * 2, "key": [key, "foo"], "value": ["bar", "baz"]},
            )
            with pytest.raises(InvalidRunMeta, match=r"Illegal meta key\(s\): " + key):
                platform.backend.meta.bulk_upsert(df)

    def test_entry_unique(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        platform.backend.meta.create(run.id, "Key", "Value")

        with pytest.raises(RunMetaEntry.NotUnique):
            platform.backend.meta.create(run.id, "Key", "Value")

        with pytest.raises(RunMetaEntry.NotUnique):
            platform.backend.meta.create(run.id, "Key", 1)

    def test_entry_not_found(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(RunMetaEntry.NotFound):
            platform.backend.meta.get(-1, "Key")

        run = platform.runs.create("Model", "Scenario")

        with pytest.raises(RunMetaEntry.NotFound):
            platform.backend.meta.get(run.id, "Key")

    def test_delete_entry(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        entry = platform.backend.meta.create(run.id, "Key", "Value")
        platform.backend.meta.delete(entry.id)

        with pytest.raises(RunMetaEntry.NotFound):
            platform.backend.meta.get(run.id, "Key")

        @utils.versioning_test(platform.backend)
        def assert_versions(backend: SqlAlchemyBackend) -> None:
            expected_versions = pd.DataFrame(
                [
                    [1, "Key", "STR", 1, None, "Value", None, None, 4, 5, 0],
                    [1, "Key", "STR", 1, None, "Value", None, None, 5, None, 2],
                ],
                columns=[
                    "id",
                    "key",
                    "dtype",
                    "run__id",
                    "value_int",
                    "value_str",
                    "value_float",
                    "value_bool",
                    "transaction_id",
                    "end_transaction_id",
                    "operation_type",
                ],
            ).replace({np.nan: None})

            vdf = backend.meta.versions.tabulate()
            utils.assert_unordered_equality(expected_versions, vdf, check_dtype=False)

    def test_list_entry(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        run.set_as_default()

        for key, value, _ in TEST_ENTRIES:
            entry = platform.backend.meta.create(run.id, key, value)

        entries = platform.backend.meta.list()

        for (key, value, type), entry in zip(TEST_ENTRIES, entries):
            assert entry.key == key
            assert entry.value == value
            assert entry.dtype == type

    def test_tabulate_entry(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        run.set_as_default()

        for key, value, _ in TEST_ENTRIES:
            platform.backend.meta.create(run.id, key, value)

        true_entries = TEST_ENTRIES_DF.copy()
        true_entries["run__id"] = run.id

        entries = platform.backend.meta.tabulate()
        utils.assert_unordered_equality(entries, true_entries)

    def test_tabulate_entries_with_run_filters(self, platform: ixmp4.Platform) -> None:
        run1 = platform.runs.create("Model", "Scenario")
        run1.set_as_default()
        run2 = platform.runs.create("Model 2", "Scenario 2")

        # Splitting the loop to more easily correct the id column below
        for key, value, _ in TEST_ENTRIES:
            platform.backend.meta.create(run1.id, key, value)
        for key, value, _ in TEST_ENTRIES:
            platform.backend.meta.create(run2.id, key, value)

        true_entries1 = TEST_ENTRIES_DF.copy()
        true_entries1["run__id"] = run1.id
        true_entries2 = TEST_ENTRIES_DF.copy()
        true_entries2["run__id"] = run2.id
        # Each entry enters the DB this much after those from run1
        true_entries2.loc[:, "id"] += len(TEST_ENTRIES)

        expected = pd.concat([true_entries1, true_entries2], ignore_index=True)

        utils.assert_unordered_equality(
            platform.backend.meta.tabulate(run={"default_only": False}), expected
        )

        utils.assert_unordered_equality(
            platform.backend.meta.tabulate(run={"is_default": True}), true_entries1
        )

        utils.assert_unordered_equality(
            platform.backend.meta.tabulate(
                run={"is_default": False, "default_only": False}
            ),
            true_entries2,
        )

    def test_tabulate_entries_with_key_filters(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        run.set_as_default()

        for key, value, _ in TEST_ENTRIES:
            platform.backend.meta.create(run.id, key, value)

        # Select just some key from TEST_ENTRIES
        key = TEST_ENTRIES[1][0]

        true_entry = TEST_ENTRIES_DF.loc[TEST_ENTRIES_DF["key"] == key].copy()
        true_entry["run__id"] = run.id

        entry = platform.backend.meta.tabulate(key=key)

        utils.assert_unordered_equality(entry, true_entry, check_dtype=False)

    def test_entry_bulk_operations(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        run.set_as_default()

        entries = TEST_ENTRIES_DF.copy()
        entries["run__id"] = run.id

        # == Full Addition ==
        platform.backend.meta.bulk_upsert(entries.drop(columns=["id", "dtype"]))
        ret = platform.backend.meta.tabulate()
        utils.assert_unordered_equality(entries, ret)

        # == Partial Removal ==
        # Remove half the data
        remove_data = entries.head(len(entries) // 2).drop(
            columns=["value", "id", "dtype"]
        )
        remaining_data = entries.tail(len(entries) // 2).reset_index(drop=True)
        platform.backend.meta.bulk_delete(remove_data)

        ret = platform.backend.meta.tabulate()
        utils.assert_unordered_equality(remaining_data, ret)

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
            columns=["id", "key", "value", "dtype"],
        )
        updated_entries["run__id"] = run.id

        platform.backend.meta.bulk_upsert(updated_entries.drop(columns=["id", "dtype"]))
        ret = platform.backend.meta.tabulate()

        utils.assert_unordered_equality(updated_entries, ret, check_like=True)

        # == Full Removal ==
        remove_data = entries.drop(columns=["value", "id", "dtype"])
        platform.backend.meta.bulk_delete(remove_data)

        ret = platform.backend.meta.tabulate()
        assert ret.empty

        @utils.versioning_test(platform.backend)
        def assert_versions(backend: SqlAlchemyBackend) -> None:
            expected_versions = pd.DataFrame(
                [
                    # == Full Addition ==
                    [1, "Boolean", "BOOL", None, None, None, True, 1, 6, 10, 0],
                    [1, "Float", "FLOAT", None, None, 0.2, None, 2, 7, 10, 0],
                    [1, "Integer", "INT", 1.0, None, None, None, 3, 8, 13, 0],
                    [1, "String", "STR", None, "Value", None, None, 4, 9, 14, 0],
                    # == Partial Removal ==
                    [1, "Boolean", "BOOL", None, None, None, True, 1, 10, None, 2],
                    [1, "Float", "FLOAT", None, None, 0.2, None, 2, 10, None, 2],
                    # == Partial Update / Partial Addition ==
                    [1, "Boolean", "FLOAT", None, None, -9.9, None, 5, 11, 15, 0],
                    [1, "Float", "FLOAT", None, None, -9.9, None, 6, 12, 15, 0],
                    [1, "Integer", "FLOAT", None, None, -9.9, None, 3, 13, 15, 1],
                    [1, "String", "FLOAT", None, None, -9.9, None, 4, 14, 15, 1],
                    # == Full Removal ==
                    [1, "Boolean", "FLOAT", None, None, -9.9, None, 5, 15, None, 2],
                    [1, "Float", "FLOAT", None, None, -9.9, None, 6, 15, None, 2],
                    [1, "Integer", "FLOAT", None, None, -9.9, None, 3, 15, None, 2],
                    [1, "String", "FLOAT", None, None, -9.9, None, 4, 15, None, 2],
                ],
                columns=[
                    "run__id",
                    "key",
                    "dtype",
                    "value_int",
                    "value_str",
                    "value_float",
                    "value_bool",
                    "id",
                    "transaction_id",
                    "end_transaction_id",
                    "operation_type",
                ],
            ).replace({np.nan: None})
            vdf = backend.meta.versions.tabulate()
            utils.assert_unordered_equality(expected_versions, vdf, check_dtype=False)

    def test_meta_bulk_exceptions(self, platform: ixmp4.Platform) -> None:
        entries = pd.DataFrame(
            [
                ["Boolean", -9.9],
                ["Float", -9.9],
                ["Integer", -9.9],
                ["String", -9.9],
            ],
            columns=["key", "value"],
        )
        run = platform.runs.create("Model", "Scenario")
        entries["run__id"] = run.id

        duplicated_entries = pd.concat([entries] * 2, ignore_index=True)

        with pytest.raises(RunMetaEntry.NotUnique):
            platform.backend.meta.bulk_upsert(duplicated_entries)

        entries["foo"] = "bar"

        with pytest.raises(SchemaError):
            platform.backend.meta.bulk_upsert(entries)

        entries = entries.drop(columns=["value"])
        with pytest.raises(SchemaError):
            platform.backend.meta.bulk_delete(entries)
