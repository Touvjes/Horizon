"""
    The function must include a check to confirm that each input file has the same headers/columns.
    The headers/columns of each file must be in the same order. If not, the columns must be re-ordered for consistency.
    The function must combine each file in (all_file_paths)
    The output file must be in tab-delimited format.
"""


import pandas as pd
import os
import logging


def check_col_names(df1: pd.DataFrame,df2: pd.DataFrame) -> None:
    """
    Checks if set of column names of two dataframes match. Logs and exits with error if not matching.
    """
    if set(df1.columns) != set(df2.columns):
        logging.info(f"{set(df1.columns)} does not match {set(df2.columns)}")
        exit(1)
    return

def check_dtypes(df1: pd.DataFrame,df2: pd.DataFrame) -> None:
    """
    Checks if data types match based on column name regardless of index. Logs and exits with error if not matching.
    """
    for column in df1.columns:
        if df1[column].dtype != df2[column].dtype:
            logging.info(f"Data type mismatch on {column}. Expected {df1[column].dtype}, received {df2[column].dtype}")
            exit(1)
    return


def check_paths(paths: list[str]) -> None:
    if not paths:
        logging.info("No argument provided, only lists are allowed")
        exit(1)
    if not isinstance(paths, list):
        logging.info(f"{type(paths)} only lists are allowed")
        exit(1)
    for path in paths:
        if not os.path.exists(path):
            logging.info(f'{path} does not exist')
            exit(1)
    return


def combine_CSVs(all_file_paths: list[str]) -> None:
    check_paths(all_file_paths)                     ##checks arguement datatype and validity of paths

    try:
        df_final = pd.read_csv(all_file_paths[0])       ##intialize with first csv in path list
    except:
        logging.info(f'Failed to read {all_file_paths[0]}')
        exit(1)

    columns = df_final.columns.tolist()

    for path in all_file_paths[1::]:                ##iterate from the second csv to end
        df_temp = pd.read_csv(path)
        check_col_names(df_final, df_temp)
        check_dtypes(df_final, df_temp)
        df_temp = df_temp[columns]                  ##rearranges next df columns to match first df order

        df_final = pd.concat([df_final, df_temp])   ##concat to final

    df_final.to_csv('combined', sep='\t')           ##output to file as tab delimited

    return


#driver
if __name__ == "__main__":
    dir = '/Users/jon/PycharmProjects/Horizon/CSVs/'
    files = os.listdir(dir)
    paths = [dir+file for file in files]
    combine_CSVs(paths)
