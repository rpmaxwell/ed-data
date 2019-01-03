from io import BytesIO
import json
import pandas as pd
import requests
import zipfile


def get_excel(data, import_params):
    '''
    read an excel table into a pandas dataframe according to import parameters
    '''
    if 'converters' in import_params.keys():
        dtype_map = {'converters': {k: eval(v) for k, v in import_params['converters'].items()}}
        import_params = {**import_params, **dtype_map}
    data_file = pd.read_excel(data, **import_params)
    return data_file


def get_csv(data, import_params):
    if 'dtype' in import_params.keys():
        dtype_map = {'dtype': {k: eval(v) for k, v in import_params['dtype'].items()}}
        import_params = {**import_params, **dtype_map}
    data_file = pd.read_csv(data, **import_params)
    return data_file


def unzip_file(download_url, filename, zip_import_parameters):
    zip_url = '{}{}'.format(download_url, zip_import_parameters['zip_filename'])
    r = requests.get(zip_url)
    r.raise_for_status()    
    zip_data = zipfile.ZipFile(BytesIO(r.content))
    if 'zip_file_path' in zip_import_parameters:
        archive_filepath = '{}{}'.format(zip_import_parameters['zip_file_path'], filename)
        data = zip_data.open(archive_filepath)
    else:
        data = zip_data.open(filename)
    return data


def extract_df(row, df_cleaning_f=None):
    root_download_url = row['root_download_url']
    filename = row['filename']
    if row['zip_import_parameters']:
        zip_params = row['zip_import_parameters']
        read_io = unzip_file(root_download_url, filename, zip_params)
    else:
        read_io = '{}{}'.format(root_download_url, filename)
    file_import_params = row['pandas_dataframe_import_paramameters']
    if row['file_format'] == 'csv':
        df = get_csv(read_io, file_import_params)
    elif row['file_format'] == 'excel':
        df = get_excel(read_io, file_import_params)
    if df_cleaning_f:
        df = df_cleaning_f(df)
    df.columns = [col.lower()\
                    .replace('-', '_')\
                    .replace(' ', '_')\
                    .replace('(', '')\
                    .replace(')', '')\
                    .replace('%', 'pct')\
                    .replace('/', '_') for col in df.columns]
    return df
