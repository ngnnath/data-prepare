import argparse
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

from converter.exception.ReadDataException import ReadDataException
from converter.exception.UnsupportedFormat import UnsupportedFormatException, DirectoryNotFoundException

""" Verify if given path is a csv file.
:param filename: file path
:type filename: str
:returns: filename path if it's valid
:raises UnsupportedFormatException: raises if the file is not supported
"""


def is_valid_csv(filename):
    if not filename.endswith(".csv"):
        raise UnsupportedFormatException("file format is not supported by the application yet.")
    return filename


""" Verify if given path is a directory.
:param path_directory: directory path
:type path_directory: str
:returns: directory path if it's valid
"""


def is_valid_directory(path_directory):
    if not os.path.isdir(path_directory):
        raise DirectoryNotFoundException("not a directory.")
    return path_directory


""" Verify_schema.
:param columns: columns of the csv
:type columns: List of str
:param header: header expected
:type header:  List of str
:return: a boolean if it's a good schema
"""


def verify_schema(columns, header):
    if len(columns) == len(header) and set(columns).issubset(header):
        return True
    return False


"""Convert csv to parquet.
:param filepath: path of the csv file
:type filepath: str
:param header: schema of the file
:type header: str
:param chunksize: size row
:type chunksize: int optional
:return 2 pandas.Dataframe:valid_df, invalid_df
:raise ReadDataException: file has no data 
"""


def convert_csv_to_parquet(filepath, header, **kwargs):
    chunksize = kwargs.get('chunksize')
    output = kwargs.get('output')
    try:
        dataframe = pd.read_csv(filepath)
        columns = dataframe.columns.tolist()
        if verify_schema(columns, header):
            dataframe = pd.read_csv(filepath, chunksize=chunksize)
            if len(dataframe) != 0:
                basename = Path(filepath).stem
                if chunksize is not None:
                    nb_part = 0
                    for chunk_part in dataframe:
                        valid_df, invalid_df = filter_data(chunk_part)
                        save_dataframe_to_parquet_format(basename, valid_df, invalid_df, ind=nb_part + 1, output=output)
                else:
                    valid_df, invalid_df = filter_data(dataframe)
                save_dataframe_to_parquet_format(basename, valid_df, invalid_df, output=output)
            else:
                logging.info('file : {} have no data to process'.format(filepath))
        else:
            logging.warning('header of the csv {} is invalid : {} \n expected {}'.format(filepath, columns, header))
            return None
    except EmptyDataError as e:
        raise ReadDataException("No data Found in file {}".format(filepath), e)


""" Verify if it's a valid row.
:param row: line to analyse 
:type row: pandas.Series
:return boolean
"""


def valid_row(row):
    if not pd.isna(row.get('image')):
        return True
    return False


"""Filter data in two files.
:param dataframe: dataframe to process.
:type dataframe: pyarrow.Dataframe
:return 2 pandas.Dataframe:valid_df, invalid_df
"""


def filter_data(dataframe):
    # easy way
    # invalid_df = dataframe[dataframe.image.isnull()]
    # valid_df = dataframe[dataframe.image.notnull()]
    valid_list = []
    invalid_list = []
    for ind, row in dataframe.iterrows():
        if valid_row(row):
            valid_list.append(row)
        else:
            invalid_list.append(row)

    invalid_df = pd.DataFrame(invalid_list)
    valid_df = pd.DataFrame(valid_list)
    return valid_df, invalid_df


""" Write data in parquet file.
:param base_name: file name
:type base_name: str
:param valid_df: valid dataframe
:type valid_df: dataframe
:type invalid_df: invalid datafram
:type invalid_df: dataframe
:param indice: indice 
:type indice: int, optional
"""


def save_dataframe_to_parquet_format(base_name, valid_df, invalid_df, **kwargs):
    date = datetime.now()
    timestamp = "{}{}{}_{}_{}_{}".format(date.year, date.month, date.day, date.hour, date.minute, date.second)
    name = timestamp + '_' + base_name
    output = kwargs.get('output')
    if output is None:
        output = './'
    logging.info('Saving files on directory "{}"'.format(output))
    if kwargs.get('ind'):
        ind = kwargs['ind']
        valid_path_file = output + name + '_valid_' + str(ind) + '.parquet'
        error_path_file = output + name + '_rejected_' + str(ind) + '.parquet'
        valid_df.to_parquet(valid_path_file)
        invalid_df.to_parquet(error_path_file)
        logging.info('2 files saved "{}" and "{}"'.format(valid_path_file, error_path_file))
    else:
        valid_path_file = output + name + '_valid.parquet'
        error_path_file = output + name + '_rejected.parquet'
        valid_df.to_parquet(valid_path_file)
        invalid_df.to_parquet(error_path_file)
        logging.info('2 files saved  "{}" and "{}"'.format(valid_path_file, error_path_file))
    return valid_path_file, error_path_file


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="file to convert", type=is_valid_csv, required=False)
    parser.add_argument("-D", "--directory", help="directory of csv to convert", type=is_valid_directory,
                        required=False)
    parser.add_argument("-c", "--chunksize", help="chunksize", type=int, required=False, default=None)
    parser.add_argument("-s", "--schema", help="schema of data expected", required=False)
    parser.add_argument("-o", "--output", help="directory to storage the parquet file", type=str, required=False,
                        default=None)
    args = parser.parse_args()

    if args.schema:
        file = open(args.schema, "r")
        columns = file.read().split(",")
    else:
        # default schema
        columns = [
            "brand",
            "category_id",
            "comment",
            "currency",
            "description",
            "image",
            "year_release"
        ]

    header = columns

    if not (args.file or args.directory):
        parser.error('at least one of this options are required : --file OR --directory.')
        exit()
    elif args.directory is not None:
        logging.info('Running data-prepare on directory {}'.format(args.directory))
        start = time.perf_counter()
        for filename in os.listdir(args.directory):
            logging.info('processing file {}'.format(args.directory))
            convert_csv_to_parquet(args.file, header, args.chunksize)
        stop = time.perf_counter()
    elif args.file is not None:
        logging.info('Running data-prepare on file "{}"'.format(args.file))
        start = time.perf_counter()
        convert_csv_to_parquet(args.file, header, chunksize=args.chunksize, output=args.output)
        stop = time.perf_counter()
    logging.info(f"time to convert files in parquet format : {stop - start:0.4f} seconds")
