import pandas as pd
import numpy as np
from dconn import Conn

import os
#os.system("python3 /Users/pto845/Documents/eddata/dconn.py")
conn = Conn('eddata', 'dw') 


def get_nns_raw_data():
    q = """
    SELECT
            sft.school_id
            ,nns.*
    FROM
            raw_data.nces_national_survey nns
    LEFT JOIN
            staging.school_ft sft
        on sft.nces_school_id = nns.ncessch
    WHERE
            sft.location_state = 'WI';
    """
    df = conn.execute_raw_query(q)
    return df


#get demographic values
def get_demographic_map():
    q = """
    SELECT
            *
    FROM
            staging.demographic_dm
    """
    df = conn.execute_raw_query(q)
    return df


#combine rows of demographic_dm with columns of raw data
def combine_nns_x_demographic_dm(nns_data, demographic_dm):
    df = pd.melt(
        nns_data
        ,id_vars=['school_id', 'survyear', 'file_id']
        ,value_vars=demographic_dm['raw_demographic_label'].values
        ,var_name='demographic_value'
        ,value_name='student_count'
    )
    #standardize school year format
    df['school_year'] = df['survyear']\
        .replace(2010, '2009-2010')\
        .replace(2011, '2010-11')\
        .replace(2012, '2011-12')\
        .replace(2013, '2012-13')
    df.drop('survyear', axis=1, inplace=True)
    return df


def convert_to_fk(df, fields_df):
#convert demographic values to fk
    df = df.merge(fields_df, left_on='demographic_value', right_on='raw_demographic_label', how='left')\
        [df.columns.values.tolist() + ['demographic_id']]\
    .drop('demographic_value', axis=1)
    return df



def upsert_enrollment_dm(df, batch_size=5000):
    df = df.dropna(how='any')
    q = """
    INSERT INTO
        staging.enrollment_dm (
            school_id
            ,file_id
            ,student_count
            ,school_year
            ,demographic_id


        )
        VALUES (
            :school_id
            ,:file_id
            ,:student_count
            ,:school_year
            ,:demographic_id
        )
    ON CONFLICT DO NOTHING
    """
    start = 0
    while start + batch_size < len(df):
        conn.execute_raw_query(q, params=df.iloc[start:start+batch_size].to_dict(orient='records'))
        start = start + batch_size
        print(start)


nns_df = get_nns_raw_data()
fields_df = get_demographic_map()
df = combine_nns_x_demographic_dm(nns_df, fields_df)
df = convert_to_fk(df, fields_df)
print('About to start uploading {} rows...'.format(len(df)))
upsert_enrollment_dm(df)