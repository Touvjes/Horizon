"""
    The function must include a check to confirm that each input file has the same headers/columns.
    The headers/columns of each file must be in the same order. If not, the columns must be re-ordered for consistency.
    The function must combine each file in (all_file_paths)
    The output file must be in tab-delimited format.
"""


import pandas as pd
import os

def check_col_names(df1: pd.DataFrame,df2: pd.DataFrame) -> None:
    """
    Checks if set of column names of two dataframes match. Throws value error if not matching.
    """
    if set(df1.columns) != set(df2.columns):
        raise ValueError(f"{set(df1.columns)} does not match {set(df2.columns)}")
    return

def check_dtypes(df1: pd.DataFrame,df2: pd.DataFrame) -> None:
    """
    Checks if data types match based on column name regardless of index. Throws type error if not matching.
    """
    for column in df1.columns:
        if df1[column].dtype != df2[column].dtype:
            raise TypeError(f"Data type mismatch on {column}. Expected {df1[column].dtype}, received {df2[column].dtype}")
    return



def combine_CSVs(all_file_paths: list[str]) -> None:
    if not all_file_paths:
        raise TypeError("No argument provided, only lists are allowed")
    if not isinstance(all_file_paths, list):
        raise TypeError(f"{type(all_file_paths)} only lists are allowed")

    df_final = pd.read_csv(all_file_paths[0])       ##intialize with first csv in path list
    columns = df_final.columns.tolist()

    for path in all_file_paths[1::]:                ##iterate from the second csv
        df_temp = pd.read_csv(path)
        check_col_names(df_final, df_temp)
        check_dtypes(df_final, df_temp)
        df_temp = df_temp[columns]

        df_final = pd.concat([df_final, df_temp])      ##concat to final

    df_final.to_csv('combined', sep='\t')           ##output to file as tab delimited

    return


if __name__ == "__main__":
    dir = '/Users/jon/PycharmProjects/Horizon/CSVs/'
    files = os.listdir(dir)
    paths = [dir+file for file in files]
    combine_CSVs(paths)
