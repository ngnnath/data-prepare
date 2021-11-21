import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from exception.UnsupportedFormat import UnsupportedFormatException, DirectoryNotFoundException

""" Verify if given path is a csv file.
:param filename: file path
:type filename: str
"""


def is_valid_csv(filename):
    if not filename.endswith(".csv"):
        raise UnsupportedFormatException("file format is not supported by the application yet.")
    return filename


""" Verify if given path is a directory.
:param path_directory: directory path
:type path_directory: str
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
"""


def verify_schema(columns, header):
    if len(columns) == len(header) and set(columns).issubset(header):
        return True
    return False


"""Convert csv to df.
:param filepath: path of the csv file
:type filepath: str
:param header: schema of the file
:type header: str
:param chunksize: size row
:type chunksize: int
:return valid_df, invalid_df: pandas.Dataframe
"""


def convert_csv_to_df(filepath, header, **kwargs):
    chunksize = kwargs.get('chunksize')
    output = kwargs.get('output')
    dataframe = pd.read_csv(filepath)
    columns = dataframe.columns.tolist()
    if verify_schema(columns, header):
        dataframe = pd.read_csv(filepath, chunksize=chunksize)
        basename = Path(filepath).stem
        if chunksize is not None:
            nb_part = 0
            for chunk_part in dataframe:
                invalid_df, valid_df = separe_data(chunk_part)
                save_datasets_to_parquet_format(basename, valid_df, invalid_df, ind=nb_part + 1, output=output)
        else:
            invalid_df, valid_df = separe_data(dataframe)
            save_datasets_to_parquet_format(basename, valid_df, invalid_df, output=output)

    else:
        logging.warning('header of the csv is invalid : %s \n expected %s', columns, header)
        return None


def separe_data(dataframe):
    invalid_df = dataframe[dataframe.image.isnull()]
    valid_df = dataframe[dataframe.image.notnull()]
    return invalid_df, valid_df


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


def save_datasets_to_parquet_format(base_name, valid_df, invalid_df, **kwargs):
    date = datetime.now()
    timestamp = "{}{}{}_{}_{}_{}".format(date.year, date.month, date.day, date.hour, date.minute, date.second)
    name = timestamp + '_' + base_name
    output = kwargs.get('output')
    if output is None:
        output = './'
    if kwargs.get('ind'):
        ind = kwargs['ind']
        valid_df.to_parquet(output + name + '_valid_' + str(ind) + '.parquet')
        invalid_df.to_parquet(output + name + '_rejected_' + str(ind) + '.parquet')
    else:
        valid_df.to_parquet(output + name + '_valid.parquet')
        invalid_df.to_parquet(output + name + '_rejected.parquet')


if __name__ == "__main__":

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
        for filename in os.listdir(args.directory):
            convert_csv_to_df(args.file, header, args.chunksize)
    elif args.file is not None:
        convert_csv_to_df(args.file, header, chunksize=args.chunksize, output=args.output)
