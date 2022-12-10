"""
Question 2

Write a function that accepts the location of any kind of flat file (file_path) and prints out the following:

    file_name (list): Print out the file name including extension (not full URL)
    delimiter (string): Print out the delimiter of the file
    columns_and_dtypes (dictionary): Print out each column name and Redshift data type.

A list of accepted Redshift data types can be found here:
https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html

Example output:
- file_name = random_file_name.csv
- delimiter = ","
- columns_and_dtypes =  {"column_1": {"column_name": "date_time", "redshift_dtype": "timestamp"},
                        "column_2": {"column_name": "region_name", "redshift_dtype": "varchar(20)"},
                        "column_3": {"column_name": "sales_volume", "redshift_dtype": "integer"}}

Note: I have left the current function to return pandas data types instead of redshift data types.
I am unsure how one could determine a redshift datatype (e.g GEOMETRY, GEOGRAPHY) without either
A) writing a case-matching statement to try to capture the range of potential values for each data type or
B) assuming that data is already in redshift and queryable via a metadata table (e.g. PG_TABLE_DEF)

I did not want to assume B and did not have time to implement A, so I've used pandas data types for this question.

"""

import os
import csv
import pandas as pd
from dataclasses import dataclass
import logging


def check_path(path: str) -> None:
    if not path:
        logging.info("No argument provided, str is required")
        exit(1)
    if not isinstance(path, str):
        logging.info(f"{type(path)} str is required")
        exit(1)
    if not os.path.exists(path):
        logging.info(f'{path} does not exist')
        exit(1)
    return


@dataclass
class MetaData:
    file_name: str
    delimiter: str
    columns_and_dtypes: dict[dict]


def metadata_extractor(path: str) -> dict:
    check_path(path)

    file_name = os.path.basename(path)

    try:
        with open(path) as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(1024))   #read sample of file
            delimiter = dialect.delimiter                       #user csv.sniffer to determine delimiter

        df = pd.read_csv(path, delimiter=delimiter)

        cols = [col for col in df.columns]

        schema = [(col, df[col].dtypes) for col in cols]    #constructs list of (Column:Data Type) tuples for each column
    except:
        logging.info(f'Could not open and extract data from {path}')
        exit(1)



    columns_and_dtypes = {}

    index = 1
    for s in schema: #build column/dtype dictionary in the requested structure
        temp_dict = {"column_name": f"{s[0]}", "redshift_dtype": f"{s[1]}"}
        columns_and_dtypes[f"column_{index}"] = temp_dict
        index += 1

    # instantiating to a dataclass is not necessary, but makes the data structure more clear and easier to expand on
    result = MetaData(file_name, delimiter, columns_and_dtypes)

    return(f"""
- file_name = {result.file_name}
- delimiter = \"{result.delimiter}\"
- columns_and_dtypes = {result.columns_and_dtypes}
""")


if __name__ == '__main__':
    print(metadata_extractor("/Users/jon/PycharmProjects/Horizon/CSVs/AAME.csv"))
