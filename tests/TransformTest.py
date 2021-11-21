import os
import unittest

from Transform import is_valid_csv, is_valid_directory, verify_schema, convert_csv_to_df
from exception.UnsupportedFormat import UnsupportedFormatException, DirectoryNotFoundException

"""TransformTest."""


class TransformTest(unittest.TestCase):
    """test method is valid csv with right path."""

    def test_is_valid_csv_true(self):
        file_path = "text.csv"
        self.assertEqual(file_path, is_valid_csv(file_path))

    """test method is valid csv raise error."""

    def test_is_valid_csv_false(self):
        file_path = "text.txt"
        with self.assertRaises(UnsupportedFormatException):
            is_valid_csv(file_path)

    """test method is directory with right path."""

    def test_is_valid_directory_true(self):
        file_path = os.path.dirname(__file__)
        self.assertEqual(file_path, is_valid_directory(file_path))

    """test method is directory with wrong path."""

    def test_is_valid_directory_error(self):
        file_path = os.path.dirname(__file__) + "/test.txt"
        with self.assertRaises(DirectoryNotFoundException):
            is_valid_directory(file_path)

    """test method verif schema."""

    def test_verify_schema(self):
        columns_to_check = ['brand', 'id']
        columns_expected = ['id', 'brand']
        self.assertTrue(verify_schema(columns_expected, columns_to_check))

    """test method verif schema with different length list."""

    def test_verify_schema_dif_length(self):
        columns_to_check = ['brand', 'id']
        columns_expected = ['id', 'brand', 'image']
        self.assertFalse(verify_schema(columns_expected, columns_to_check))

    """test method verif schema with different column name."""

    def test_verify_schema_dif(self):
        columns_to_check = ['brand', 'ids']
        columns_expected = ['id', 'brand']
        self.assertFalse(verify_schema(columns_expected, columns_to_check))

    """test method convert csv to dataframe."""

    def test_convert_csv_to_df_right_format(self):
        # schema
        columns = [
            "brand",
            "category_id",
            "comment",
            "currency",
            "description",
            "image",
            "year_release"
        ]

        path = os.path.join(os.path.dirname(__file__), 'resources', 'right_example.csv')
        convert_csv_to_df(path, columns)

    """test method convert csv to dataframe with wrong file."""

    def test_convert_csv_to_df_wrong_format(self):
        # schema
        columns = [
            "brand",
            "category_id",
            "comment",
            "currency",
            "description",
            "image",
            "year_release"
        ]

        path = os.path.join(os.path.dirname(__file__), 'resources', 'wrong_example.csv')
        self.assertIsNone(convert_csv_to_df(path, columns))


if __name__ == '__main__':
    unittest.main()
