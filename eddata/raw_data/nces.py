import copy
import json
import os
import pandas as pd
from dconn import Conn
import load

# get source metadata
source_data = {
    "home_url": "https://nces.ed.gov/",
    "source_name": "NCES",
    "publisher": "National Center For Education Statistics"
}

with open(os.environ['FILE_RECORD_TEMPLATE']) as f:
    file_record_format_d = json.loads(f.read())

d = {
    '2017-18': {
                'directory': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1718_w_0a_03302018_csv.zip'
    }
    '2016-17': {
                'directory': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1617_w_0e_050317_csv.zip'
    },
    '2015-16': {
                'directory': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1516_w_2a_011717_csv.zip',
                'membership': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_052_1516_w_2a_011717_csv.zip',
                'staff_survey': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_059_1516_w_2a_011717_csv.zip',
                'school_characteristics': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_129_1516_w_2a_011717_csv.zip',
                'lunch_program': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_033_1516_w_2a_011717_csv.zip'
    },
    '2014-15': {
                'directory': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1415_w_0216601a_txt.zip',
                'membership': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_052_1415_w_0216161a_txt.zip',
                'staff_survey': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_059_1415_w_0216161a_txt.zip',
                'school_characteristics': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_129_1415_w_0216161a_txt.zip',
                'lunch_program': 'https://nces.ed.gov/ccd/Data/zip/ccd_sch_033_1415_w_0216161a_txt.zip'
    },
    '2013-14': {
                'national_survey': 'https://nces.ed.gov/ccd/Data/zip/sc132a_txt.zip'
    },
    '2012-13': {
                'national_survey': 'https://nces.ed.gov/ccd/Data/zip/sc122a_txt.zip'
    },
    '2011-12': {
                'national_survey': 'https://nces.ed.gov/ccd/Data/zip/sc111a_supp_txt.zip'
    },
    '2010-11': {
                'national_survey': 'https://nces.ed.gov/ccd/Data/zip/sc102a_txt.zip'            

    },
    '2009-10': {
                'national_survey': 'https://nces.ed.gov/ccd/data/zip/sc092a_txt.zip'            

    }
}





def get_national_survey(file_record_template):
    file_list = []
    topic = 'national_survey'
    for year in d.keys():
        if topic not in d[year].keys():
            continue
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url'] = 'https://nces.ed.gov/ccd/Data/zip/'
        file_record['filename'] = d[year][topic].replace(file_record['root_download_url'], '').replace('_txt.zip', '.txt')
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = d[year][topic].replace(file_record['root_download_url'], '')
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["M", "-1", "N", "-2", "-9"],
            "dtype": {
                "NCESSCH": "str",
                "ST_SCHID": "str",
                "LEAID": "str",
                "ST_LEAID": "str",
                "PHONE": "str",
            },
             "sep": "\t",
             "encoding": "ISO-8859-1"
        }
        file_list.append(file_record)
    return file_list

conn = Conn('eddata', 'raw_data')

# create source record
load.source_to_db(conn, source_data)
source_id = 4

national_survey = get_national_survey(file_record_format_d)
# load.file_records_to_db(universe_survey, source_id)


def create_data_table(topics_to_populate):
    if isinstance(topics_to_populate, str):
        topics_to_populate = (topics_to_populate, )
    file_records_q = """
        SELECT 
                s.source_name
                ,f.*
        FROM 
                raw_data.file_ft f
        JOIN
                raw_data.source_dm s
            ON s.source_id = f.source_id
        WHERE f.source_id = 4
    AND f.topic IN {}
    """.format(tuple(topics_to_populate))
    file_records = conn.execute_raw_query(file_records_q, fmt='dict')
    return file_records
    # for rec in file_records:
    #     load.data_file_to_db(rec, remove_reserved_col_names)

def get_staff_survey():
    file_list = []
    topic = 'staff_survey'
    for year in d.keys():
        if topic not in d[year].keys():
            continue
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url'] = 'https://nces.ed.gov/ccd/Data/zip/'
        file_record['filename'] = d[year][topic].replace(file_record['root_download_url'], '').replace('_txt.zip', '.txt')
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = d[year][topic].replace(file_record['root_download_url'], '')
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "dtype": {
                "NCESSCH": "str",
                "LEAID": "str"
            },
             "encoding": "ISO-8859-1"
        }
        file_list.append(file_record)
    return file_list


# remove_reserved_col_names = lambda df: df[[i for i in df.columns if i.lower() not in ('union')]]


survey_records = file_records('national_survey')
staff_records = file_records('staff_survey')

topics = [survey_records, staff_records]

for topic in topics:
    df = pd.DataFrame()
    for rec in topic:
        z = load.create_dataframe_for_db(rec)
        z.leanm = z.leanm.str.replace('\t', '')
        df = pd.concat([df, z], sort=False)
        print(z.head())
    load.dataframe_to_db(df, topic['topic'])
