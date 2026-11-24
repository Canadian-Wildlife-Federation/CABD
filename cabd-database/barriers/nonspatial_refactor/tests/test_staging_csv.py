import unittest

from cabd_updates.staging.csv_utils import is_blank_csv_row
from cabd_updates.staging.stage_updates import (
    _build_promotion_insert_sql,
    _new_feature_fingerprint,
    _promote_raw_rows_to_staging,
)


class BlankCsvRowTests(unittest.TestCase):
    def test_blank_rows_include_empty_and_whitespace_cells(self):
        self.assertTrue(is_blank_csv_row([]))
        self.assertTrue(is_blank_csv_row(["", "  ", "\t"]))
        self.assertFalse(is_blank_csv_row(["", " dam "]))


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
            "status",
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
        self.assertNotIn('existing."status"', sql)

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
