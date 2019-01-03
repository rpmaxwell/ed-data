import copy
import json
import os
from psycopg2.extras import Json

from dconn import Conn
import load


template_path = '/Users/pto845/ed-data/raw_data/file_import/file_record_format.json'
with open(template_path) as f:
    file_record_format_d = json.loads(f.read())


source_data = {
            "home_url": "https://www.louisianabelieves.com",
            "source_name": "louisiana_believes",
            "publisher": "Louisiana Department of Education"
        }

def create_public_enrollment(file_record_template):
    file_list = []
    filename_list = [
        "oct-2016-multi-stats-(mfp-by-site-and-lea).xlsx",
        "oct-2015-multi-stats-(mfp-by-site)---public.xls",
        "oct-2014-multi-stats-(mfp-by-site)---public.xls",
        "oct-2013-multi-stats-(mfp-by-site)---public.xls",
        "oct-2012-multi-stats-(mfp-by-site)---public.xls",
    ]
    for file in filename_list:
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://www.louisianabelieves.com/docs/default-source/data-management/'
        file_record['filename'] = file
        file_record['file_format'] = 'excel'
        file_record['topic'] = 'public_enrollment'
        file_record['zip_import_parameters'] = None
        if 'xlsx' in file:
             sheet_name = 'MFP by Site'
        else:
            sheet_name = "Total_bySite"
        if ('oct-2014' in file) or ('oct-2015' in file):
            header = [4, 5]
        elif ('oct-2013' in file) or ('oct-2012' in file):
            header = [3, 4]
        else:
            header = [3, 4, 5]
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA"],
            "sheet_name": sheet_name,
            "header": header,
            "index_col": None,
            "dtype": {
                "Site Code": "str",
                "LEA": "str",
                "CESA": "str"
            }
        }
        file_list.append(file_record)
    return file_list


def fix_bad_headers(df):
    '''
    horrible, hacky function to get LA's incomprehensible excel files into the same format and into the DB. 
    '''
    col_map = {
    'District / Agency Name': 'LEA Name',
    'School or Site Name': 'Site Name',
    'Total Reported Students': 'Enrollment',
    'Grade Placement Infants (Sp. Ed.)': 'Grade Placement Infants (Sp Ed)'
    }
    df.index.rename('LEA', inplace=True)
    df.reset_index(inplace=True)
    df.columns = [' '.join(col).strip() for col in df.columns]
    df.columns = [col.split(' Unnamed:', 1)[0].replace('Number', '').strip() for col in df.columns]
    try:
        df.drop('% At Risk', axis=1, inplace=True)
    except KeyError:
        pass
    df.columns = [col.replace('**', '') for col in df.columns]
    for k, v in col_map.items():
        if k in df.columns:
            df[v] = df[k]
            df.drop(k, axis=1, inplace=True)
    df.columns = [col.replace('Gtade', 'Grade') for col in df.columns]
    return df


conn = Conn('eddata', 'raw_data')

#### create source record
la_believes_source = load.source_to_db(conn, source_data)
source_id = 3

## create records for these files:
public_enrollment = create_public_enrollment(file_record_format_d)

#upload these file records
file_categories = [public_enrollment]
for row in file_categories:
    load.file_records_to_db(row, source_id)


# upload data from file list
topics_to_populate = 'public_enrollment'
file_records_q = """
    SELECT 
            s.source_name
            ,f.*
    FROM 
            raw_data.file_ft f
    JOIN
            raw_data.source_dm s
        ON s.source_id = f.source_id   
    WHERE f.source_id = 3
    AND f.topic = '{}'
""".format(topics_to_populate)

file_records = conn.execute_raw_query(file_records_q, fmt='dict')
for rec in file_records:
    load.data_file_to_db(rec, fix_bad_headers)

