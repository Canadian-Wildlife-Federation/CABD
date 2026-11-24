import csv
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cabd_updates.staging.csv_utils import (
    has_sharepoint_status,
    is_blank_csv_row,
)
from cabd_updates.staging.stage_data_sources import stage_data_sources_csv_to_table
from cabd_updates.staging.stage_updates import (
    _build_promotion_insert_sql,
    _new_feature_fingerprint,
    _promote_raw_rows_to_staging,
)
from cabd_updates.publish.publish import _derive_publish_columns


class BlankCsvRowTests(unittest.TestCase):
    def test_blank_rows_include_empty_and_whitespace_cells(self):
        self.assertTrue(is_blank_csv_row([]))
        self.assertTrue(is_blank_csv_row(["", "  ", "\t"]))
        self.assertFalse(is_blank_csv_row(["", " dam "]))


class SharePointStatusTests(unittest.TestCase):
    def test_any_nonblank_status_is_skipped(self):
        for value in ("complete", "on_hold", "anything", "in progress"):
            with self.subTest(value=value):
                self.assertTrue(has_sharepoint_status(value))

        for value in (None, "", "  ", "\t "):
            with self.subTest(value=value):
                self.assertFalse(has_sharepoint_status(value))

    def test_data_source_csv_separates_status_and_skips_nonblank_rows(self):
        class CopyCursor:
            def __init__(self, connection):
                self.connection = connection

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, query, params=None):
                self.connection.executed.append((query, params))
                return None

            def copy_expert(self, sql, buffer):
                self.connection.copy_sql = sql
                self.connection.copy_csv = buffer.getvalue()

        class CopyConnection:
            def __init__(self):
                self.copy_sql = ""
                self.copy_csv = ""
                self.executed = []

            def cursor(self):
                return CopyCursor(self)

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "sources.csv"
            csv_path.write_text(
                "data_source_short_name,status,source\n"
                "source-a,active,https://example.test/a\n"
                "source-b,on_hold,https://example.test/b\n"
                "source-c,completed,https://example.test/c\n"
                "source-d,,https://example.test/d\n"
                "source-e,   ,https://example.test/e\n",
                encoding="utf-8",
            )
            connection = CopyConnection()
            with patch(
                "cabd_updates.staging.stage_data_sources.call_scalar",
                side_effect=[0, 1],
            ):
                result = stage_data_sources_csv_to_table(connection, csv_path)

        copied_rows = list(csv.DictReader(io.StringIO(connection.copy_csv)))
        self.assertIn('"sharepoint_status"', connection.copy_sql)
        self.assertIn('"processing_status"', connection.copy_sql)
        self.assertEqual(result["dropped_sharepoint_status_rows"], 3)
        self.assertEqual(len(copied_rows), 2)
        self.assertTrue(
            all(not row["sharepoint_status"].strip() for row in copied_rows)
        )
        self.assertEqual(
            {row["processing_status"] for row in copied_rows},
            {"ready"},
        )

    def test_status_columns_are_not_published_as_feature_attributes(self):
        attributes, provenance = _derive_publish_columns(
            {
                "dam_name_en": "varchar",
                "sharepoint_status": "varchar",
                "processing_status": "varchar",
            },
            {"dam_name_en", "sharepoint_status", "processing_status"},
        )
        self.assertEqual(attributes, ["dam_name_en"])
        self.assertEqual(provenance, ["dam_name_en"])


class NewFeatureDeduplicationSqlTests(unittest.TestCase):
    def test_fingerprint_ignores_generated_id_but_changes_with_submitted_data(self):
        header = [
            "cabd_id",
            "entry_classification",
            "latitude",
            "longitude",
            "data_source_short_name",
            "dam_name_en",
        ]
        first = ["generated-id-1", "new feature", "45.0", "-75.0", "source", "Dam"]
        repeat = ["generated-id-2", "new feature", "45.0", "-75.0", "source", "Dam"]
        changed_location = [
            "generated-id-3",
            "new feature",
            "46.0",
            "-75.0",
            "source",
            "Dam",
        ]

        self.assertEqual(
            _new_feature_fingerprint(header, first),
            _new_feature_fingerprint(header, repeat),
        )
        self.assertNotEqual(
            _new_feature_fingerprint(header, first),
            _new_feature_fingerprint(header, changed_location),
        )

    def test_new_feature_fingerprint_uses_coordinates_and_submitted_fields(self):
        columns = [
            "cabd_id",
            "entry_classification",
            "latitude",
            "longitude",
            "data_source_short_name",
            "dam_name_en",
            "reviewer_comments",
            "sharepoint_status",
            "processing_status",
            "submitted_on",
        ]
        sql = _build_promotion_insert_sql(
            "cabd.dam_updates_raw",
            "cabd.dam_updates",
            columns,
            {column.lower(): column for column in columns},
            None,
        )

        self.assertIn("NOT EXISTS", sql)
        self.assertIn(
            'existing."latitude" IS NOT DISTINCT FROM incoming."latitude"',
            sql,
        )
        self.assertIn(
            'existing."longitude" IS NOT DISTINCT FROM incoming."longitude"',
            sql,
        )
        self.assertIn(
            'existing."reviewer_comments" IS NOT DISTINCT FROM incoming."reviewer_comments"',
            sql,
        )
        self.assertNotIn('existing."cabd_id"', sql)
        self.assertIn('existing."sharepoint_status"', sql)
        self.assertNotIn('existing."processing_status"', sql)

    def test_new_feature_deduplication_requires_coordinates(self):
        columns = ["entry_classification", "latitude"]
        with self.assertRaisesRegex(ValueError, "latitude and longitude"):
            _build_promotion_insert_sql(
                "cabd.dam_updates_raw",
                "cabd.dam_updates",
                columns,
                {column: column for column in columns},
                None,
            )


class _PromotionCursor:
    def __init__(self):
        self.rowcount = 0
        self.queries = []
        self._ledger_inserts = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params=None):
        del params
        self.queries.append(query)
        normalized_query = query.lstrip()
        if normalized_query.startswith("SELECT raw_id"):
            self.rowcount = -1
        elif "SELECT entry_classification = 'new feature'" in normalized_query:
            self.rowcount = 1
        elif normalized_query.startswith(
            "INSERT INTO cabd.dam_updates_import_fingerprints"
        ):
            self._ledger_inserts += 1
            self.rowcount = 1 if self._ledger_inserts == 1 else 0
        elif normalized_query.startswith("WITH incoming AS"):
            self.rowcount = 1
        else:
            self.rowcount = 1

    def fetchall(self):
        return [(1,), (2,)]

    def fetchone(self):
        return (True,)


class _PromotionConnection:
    def __init__(self):
        self.test_cursor = _PromotionCursor()

    def cursor(self):
        return self.test_cursor


class PersistentNewFeatureDeduplicationTests(unittest.TestCase):
    def test_existing_fingerprint_skips_promotion_but_marks_raw_row_loaded(self):
        connection = _PromotionConnection()
        columns = [
            "cabd_id",
            "entry_classification",
            "latitude",
            "longitude",
            "data_source_short_name",
            "dam_name_en",
        ]

        result = _promote_raw_rows_to_staging(
            connection,
            "cabd.dam_updates_raw",
            "cabd.dam_updates",
            "cabd.dam_updates_import_fingerprints",
            required_columns=columns,
            header_lc={column.lower(): column for column in columns},
            cast_map=None,
        )

        self.assertEqual(result["inserted_rows"], 1)
        self.assertEqual(result["duplicate_rows"], 1)
        self.assertEqual(result["error_rows"], 0)
        self.assertTrue(
            any(
                query.lstrip().startswith(
                    "INSERT INTO cabd.dam_updates_import_fingerprints"
                )
                and "ON CONFLICT (fingerprint) DO NOTHING" in query
                for query in connection.test_cursor.queries
            )
        )


if __name__ == "__main__":
    unittest.main()
