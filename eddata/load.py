import json
import os
from dconn import Conn
import extract
import pandas as pd
from sqlalchemy.dialects import postgresql
from psycopg2.extras import Json

conn = Conn('eddata', 'raw_data')


def source_to_db(conn, row):
    q = """
    INSERT INTO raw_data.source_dm (
    source_name
    ,publisher
    ,home_url
    ,date_added
    )
    VALUES (
    :source_name
    ,:publisher
    ,:home_url
    ,CURRENT_TIMESTAMP
    )
    ON CONFLICT ON CONSTRAINT source_dm_unique_constraint 
        DO NOTHING
    """
    conn.execute_raw_query(q, params=row)


def create_file_record(conn, row):
    q = """
    INSERT INTO raw_data.file_ft (
        filename
        ,file_format
        ,source_id
        ,topic
        ,root_download_url
        ,pandas_dataframe_import_paramameters
        ,zip_import_parameters
        ,date_added
    )
    VALUES (
        :filename
        ,:file_format
        ,:source_id
        ,:topic
        ,:root_download_url
        ,:pandas_dataframe_import_paramameters
        ,:zip_import_parameters
        ,CURRENT_TIMESTAMP
    )
    ON CONFLICT ON CONSTRAINT file_dm_unique_constraint
        DO UPDATE SET
            file_format=EXCLUDED.file_format
            ,root_download_url=EXCLUDED.root_download_url
            ,pandas_dataframe_import_paramameters=EXCLUDED.pandas_dataframe_import_paramameters
            ,zip_import_parameters=EXCLUDED.zip_import_parameters
    """
    conn.execute_raw_query(q, params=row)


def file_records_to_db(file_rows, source_id):
    for row in file_rows:
        row['zip_import_parameters'] = Json(row['zip_import_parameters'])
        row['pandas_dataframe_import_paramameters'] = Json(row['pandas_dataframe_import_paramameters'])
        row['source_id'] = source_id
        create_file_record(conn, row)


def create_dataframe_for_db(file):
    file_df = extract.extract_df(file)
    file_df.reset_index(inplace=True, drop=True)
    file_df['file_id'] = file['file_id']
    file_df['file_index'] = file_df.index
    return file_df


def dataframe_to_db(file_or_df, table_name):
    if isinstance(file_or_df, dict):
        df = create_dataframe_for_db(file_or_df)
    else:
        df = file_or_df
    if not conn.engine.dialect.has_table(conn.engine, table_name, schema=conn.schema_name):
        conn.create_table_from_columns(table_name, dict(zip(df.columns, df.dtypes)))
        create_constraint = '''
        ALTER TABLE {}.{} ADD UNIQUE {};
        '''.format(conn.schema_name, table_name, '(file_index, file_id)')
        conn.execute_raw_query(create_constraint)
    conn.copy_to_db(table_name, df, conflict_str='(file_index, file_id)')



def data_file_to_db(file, df_cleaning_f=None):
    table_name = '{}_{}'.format(file['source_name'], file['topic'])
    file.pop('source_name')
    file_df = extract.extract_df(file, df_cleaning_f)
    file_df.reset_index(inplace=True, drop=True)
    file_df['file_id'] = file['file_id']
    file_df['file_index'] = file_df.index
    ## Not breaking this into a separate function because the dtypes get all weird for some reason
    if not conn.engine.dialect.has_table(conn.engine, table_name, schema=conn.schema_name):
        conn.create_table_from_columns(table_name, dict(zip(file_df.columns, file_df.dtypes)))
        create_constraint = '''
        ALTER TABLE {}.{} ADD UNIQUE {};
        '''.format(conn.schema_name, table_name, '(file_index, file_id)')
        conn.execute_raw_query(create_constraint)
    conn.copy_to_db(table_name, file_df, conflict_str='(file_index, file_id)')

