# Question 3
#
# Explain what the following function is doing
#
import os
import logging
import csv
import json
def my_function(self, s3_key, data_df, stage_prefix, output_folder, delimiter=',', app_id='app_id'):
    """
    This function takes a dataframe and splits it into multiple files based on the client, found in the app_id column.
    Ultimately, the function writes parts of the df to different locations based on the client.
    It creates a config file path based on the class attribute instance 'config_path' and  'app_map.json'
    If the path does not already exist, the function logs and exits with 1, indicating an error.
    Iterrows() is used to iterate over rows/columns of the dataframe.
    app_id is matched to the values of an existing client dictionary, if such client is not present in the dict already
    the client is added to the dictionary.
    A file name is constructed based on the given output_folder argument, client, and s3_key and added to the files list
    if that file name is not already present.
    the file name is then opened and used as the file input to declare a new CSV.DictWriter class instance named writer.
    the writer then writes a header to the file if the file size is 0kb.
    A dictionary named data is constructed using list comprehension which contains column:value pairs for each column,
    value in a zipped list of header + row. The zip function takes a list of headers and a list of rows of equal length
    and constructs tuples combining elements at the same index of each of those lists for each row.
    The 'data' dictionary is then written to the open file.

    """
    config_file = os.path.join(self.config_path, 'app_map.json')
    if os.path.exists(config_file):
        with open(config_file) as json_file:
            map_data = json.load(json_file)
    else:
        logging.info('missing config file for mapping')
        exit(1)

    clients = []
    new_s3_path = []
    files = []
    filename = None
    for index, row in data_df.iterrows():
        headers = list(data_df.columns)
        for client in map_data.keys():
            if row[app_id].startswith(client) or row[app_id] in map_data[client]:
                if client not in clients:
                    clients.append(client)
                filename = f'{output_folder}/{client}-{os.path.splitext(s3_key)[0]}.csv'
                if filename not in files:
                    files.append(filename)
                with open(filename, 'a', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, delimiter=delimiter,
                                            lineterminator='\n',
                                            fieldnames=headers,
                                            quotechar='"')
                    if os.stat(filename).st_size == 0:
                        writer.writeheader()
                    data = {col: val for col, val in zip(headers, list(row))}
                    writer.writerow(data)