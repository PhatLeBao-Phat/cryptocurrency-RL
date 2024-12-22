# Import
import pytest
import pandas as pd
from dev.scripts.pipeline import MySQLLoader


# ----------------------------------------------
# MySQLLoader Test Cases
# ----------------------------------------------
class TestMySQLLoader:
    def test_match_columns(self):
        # Case 1: Columns match
        df = pd.DataFrame({"col1": [1], "col2": [2]})
        cols = ["col1", "col2"]
        assert MySQLLoader.match_columns(df, cols) is True

        # Case 2: Columns don't match
        df = pd.DataFrame({"col1": [1], "col123": [2]})
        cols = ["col1", "col2"]
        assert MySQLLoader.match_columns(df, cols) is False

    def test_filter_exists(self):
        # Mock `_filter_exists` method if not yet implemented
        db_df = pd.DataFrame({"col1": ["str1", "str2"], "col2": [1, 2]})
        df = pd.DataFrame({"col1": ["str1", "str3"]})

        result = MySQLLoader._filter_exists(
            df = df, 
            db_df = db_df, 
            unique_key="col1"
        )
        expected = pd.DataFrame({"col1": ["str3"]})

        pd.testing.assert_frame_equal(result, expected)


# ----------------------------------------------
# Pipeline Test Cases (To Be Implemented)
# ----------------------------------------------
class TestPipeline:
    def test_pipeline_execution(self):
        # Implement pipeline tests
        pass
