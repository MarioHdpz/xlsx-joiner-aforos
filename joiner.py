"""Module to join xlsx files - Traffic volume and speed """

from concurrent.futures import process
import datetime
import multiprocessing
import os
import re

import pandas

current_path = os.getcwd()

def get_xlsx_files(directory):
    files_extension = "xlsx"
    xlsx_files = list()
    year_dirs = os.listdir(f"{current_path}/{directory_name}")
    year_dirs.sort()
    for year_dir in year_dirs:
        file_names = os.listdir(f"{current_path}/{directory_name}/{year_dir}")
        file_names.sort()
        for file_name in file_names:
            if re.match(".*\.xlsx$", file_name):
                xlsx_files.append(
                    f"{current_path}/{directory_name}/{year_dir}/{file_name}"
                )
    return xlsx_files


def get_volume_df(filepath):
    volume = pandas.read_excel(filepath, engine='openpyxl', sheet_name=0)
    # Remove first usless header
    df = volume.drop([0])
    df.columns = range(df.shape[1])

    def get_street_id(x):
        match =  re.search('^(.*) \|', x)
        if match:
            return match.group(1)

    # Get header rows and fill null values
    headers = df[0:3].copy()
    headers.iloc[0] = headers.iloc[0].apply(lambda x: None if x == "Total" else x)
    headers = headers.apply(lambda x: x.ffill(), axis=1)
    headers.iloc[1] = headers.iloc[1].apply(get_street_id)

    cameras = df.iloc[3].fillna("Total")

    # Concatenate all headers in one row
    headers = headers.append(cameras)
    column_names = headers.loc[1] + "_" + headers.loc[2] + " " +headers.loc[4]
    column_names[0] = "date"
    column_names[1] = "time"
    values = df[[isinstance(x, datetime.datetime) for x in df[0]]].copy()
    values.columns = column_names
    values.reset_index(inplace=True, drop=True)
    values['date'] = [d.date() for d in values['date']]
    return values

def get_speed_df(filepath):
    speed = pandas.read_excel(filepath, engine='openpyxl', sheet_name=1)
    df = speed.drop([0])
    df.columns = range(df.shape[1])
    df = df.transpose()
    values = df[[isinstance(x, datetime.datetime) for x in df[1]]].copy()
    column_names = df.iloc[0].apply(lambda x: "V_" + str(x))
    column_names[1] = "datetime"
    values.columns = column_names
    values['date'] = [d.date() for d in values['datetime']]
    values['time'] = [d.time() for d in values['datetime']]
    del values["datetime"]
    return values


def get_volume_and_speed_df(filename):
    volume_df = get_volume_df(filename)
    speed_df = get_speed_df(filename)
    merged = volume_df.merge(
        speed_df,
        how="left",
        left_on=["date", "time"],
        right_on=["date", "time"]
    )
    return merged

def get_dataframe(filename, processed_dataframes):
    try:
        df = get_volume_and_speed_df(filename)
        processed_dataframes.append(df)
    except Exception as error:
        print(f"Error in file {filename}: {error}")


if __name__ == "__main__":
    directory_name = "AFOROS"

    xlsx_files = get_xlsx_files(directory_name)
    
    with multiprocessing.Manager() as manager:
        processed_dataframes = manager.list()
        jobs = []
        for xlsx_file in xlsx_files:
            p = multiprocessing.Process(target=get_dataframe, args=(xlsx_file, processed_dataframes))
            jobs.append(p)
            p.start()

        for j in jobs:
            j.join()

        integrated_df = pandas.DataFrame()
        for dataframe in processed_dataframes:
            integrated_df = integrated_df.append(dataframe)

    integrated_df.to_csv(f"{current_path}/output/speed_volume.csv")

