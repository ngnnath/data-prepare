import os
import unittest

import numpy as np
import pandas

from converter.csvtoparquet import is_valid_csv, is_valid_directory, verify_schema, convert_csv_to_parquet, valid_row, \
    filter_data, \
    save_dataframe_to_parquet_format
from converter.exception.ReadDataException import ReadDataException
from converter.exception.UnsupportedFormat import UnsupportedFormatException, DirectoryNotFoundException

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
        path = os.path.join(os.path.dirname(__file__), 'resources', 'right_example.csv')
        convert_csv_to_parquet(path, columns)

    """test method convert csv to dataframe with wrong file."""

    def test_convert_csv_to_df_wrong_format(self):
        path = os.path.join(os.path.dirname(__file__), 'resources', 'wrong_example.csv')
        self.assertIsNone(convert_csv_to_parquet(path, columns))

    """test method convert csv to parquet with wrong file."""

    def test_convert_csv_to_parquet_empty_file(self):
        with self.assertRaises(ReadDataException):
            path = os.path.join(os.path.dirname(__file__), 'resources', 'empty_example.csv')
            convert_csv_to_parquet(path, columns)

    """test method valid row with right value."""

    def test_valid_row(self):
        row = pandas.Series({'image': 'test'})
        self.assertTrue(valid_row(row))

    """test method valid row with NaN value."""

    def test_invalid_row_nan_value(self):
        row = pandas.Series({'images': np.nan})
        self.assertFalse(valid_row(row))

    """test method valid row with empty str."""

    def test_invalid_row_str_empty(self):
        row = pandas.Series({'images': ''})
        self.assertFalse(valid_row(row))

    """test filter data with dataframe."""

    def test_filter_data(self):
        d = [['brand1', np.nan, 'desc1'], ['brand2', 'png', 'desc2']]
        df = pandas.DataFrame(d, columns=['brand', 'image', 'description'])
        valid_df, invalid_df = filter_data(df)
        self.assertEqual(1, len(valid_df))
        self.assertEqual(1, len(invalid_df))

    """test save_dataframe_to_parquet_format().
     check if the parquet file ."""

    def test_save_dataframe_to_parquet_format(self):
        schema = ['brand', 'image', 'description']
        data1 = ['brand1', np.nan, 'desc1']
        data2 = ['brand2', 'png', 'desc2']
        d = [data1, data2]
        df = pandas.DataFrame(d, columns=schema)
        valid_df, invalid_df = filter_data(df)
        validfile_path, errorfile_path = save_dataframe_to_parquet_format('product', valid_df, invalid_df)
        invalid_result = pandas.read_parquet(errorfile_path)
        valid_result = pandas.read_parquet(validfile_path)
        expected_df1 = pandas.DataFrame([['brand1', np.nan, 'desc1']], columns=schema)
        expected_df2 = pandas.DataFrame([['brand2', 'png', 'desc2']], columns=schema)
        # self.assertTrue(invalid_result.equals(expected_df1))
        # self.assertTrue(valid_result.equals(expected_ldf2))

    '''remove the files generated by the tests.'''

    @classmethod
    def tearDownClass(self):
        for filename in os.listdir(os.getcwd()):
            if filename.endswith('.parquet'):
                os.remove(filename)


if __name__ == '__main__':
    unittest.main()
