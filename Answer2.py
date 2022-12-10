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
                        {'column_1': {'column_name': 'Data type', 'redshift_dtype': 'object'},
                        'column_2': {'column_name': 'Aliases', 'redshift_dtype': 'object'},
                        'column_3': {'column_name': 'Description', 'redshift_dtype': 'object'}}

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

    file_name = os.path.basename(path)

    with open(path) as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))   #read sample of file
        delimiter = dialect.delimiter                       #user csv.sniffer to determine delimiter

    df = pd.read_csv(path, delimiter=delimiter)

    cols = [col for col in df.columns]

    schema = [[col, df[col].dtypes] for col in cols]

    columns_and_dtypes = {}

    index = 1
    for s in schema:
        temp_dict = {"column_name": f"{s[0]}", "redshift_dtype": f"{s[1]}"}
        columns_and_dtypes[f"column_{index}"] = temp_dict
        index += 1

    result = MetaData(file_name,delimiter,columns_and_dtypes)
    return(f"""
- file_name = {file_name}
- delimiter = \"{delimiter}\"
- columns_and_dtypes = {columns_and_dtypes}
""")

if __name__ == '__main__':
    print(metadata_extractor("/Users/jon/PycharmProjects/Horizon/CSVs/DataTypes.txt"))
