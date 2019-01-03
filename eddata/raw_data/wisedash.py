import copy
import json
import os
from dconn import Conn
import load

# get source metadata
source_data = {
            "home_url": "https://dpi.wi.gov/",
            "source_name": "wisedash",
            "publisher": "Wisconsin Department of Education"
        }

# get file template from file
with open(os.environ['FILE_RECORD_TEMPLATE']) as f:
    file_record_format_d = json.loads(f.read())

'''
Helper functions
'''
def add_years_to_template(template, year):
    fall = '{}'.format(year)
    spring = '{}'.format(str(year+1)[2:])
    filename = template.format(fall, spring)
    return filename

'''
formatting import data
'''

## Public enrollment 
def create_public_enrollment(file_record_template):
    file_list = []
    filename_template = 'enrollment_certified_{}-{}.csv'
    zip_filename_template = 'enrollment_certified_{}-{}.zip'
    topic = 'public_enrollment'
    for year in range(2011, 2018):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://dpi.wi.gov/sites/default/files/wise/downloads/'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str"
            }
        }
        file_list.append(file_record)
    return file_list


## Public enrollment 
def create_aspire_test_results(file_record_template):
    file_list = []
    filename_template = 'aspire_statewide_certified_{}-{}.csv'
    zip_filename_template = 'aspire_statewide_certified_{}-{}.zip'
    topic = 'aspire_test_results'
    for year in range(2014, 2017):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://dpi.wi.gov/sites/default/files/wise/downloads/'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str"
            }
        }
        file_list.append(file_record)
    return file_list


def create_private_enrollment(file_record_template):
    file_list = []
    filename_template = 'private_enrollment_master_{}-{}.zip'
    zip_filename_template = 'private_enrollment_master_{}-{}.xlsx'
    zip_file_path_template = 'private_enrollment_master_{}-{}/'
    topic = 'private_enrollment'
    for year in range(2012, 2018):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://dpi.wi.gov/sites/default/files/wise/downloads/'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'excel'
        file_record['topic'] = topic
        file_record['pandas_dataframe_import_paramameters'] = {
            "header": [2],
            "converters": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str"
            }
        }
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = add_years_to_template(zip_file_path_template, year)
        if file_record['filename'] == 'private_enrollment_master_2017-18.xlsx':
            file_record['pandas_dataframe_import_paramameters']['sheet_name'] = 'PrEM18'
        elif file_record['filename'] == 'private_enrollment_master_2017-18.xlsx':
            file_record['pandas_dataframe_import_paramameters']['sheet_name'] = 'NEM16 DRAFT'
        else:
            sheet_year = '{}'.format(str(year+1)[2:])
            file_record['pandas_dataframe_import_paramameters']['sheet_name'] = 'NEM{}'.format(sheet_year)
        file_list.append(file_record)
    return file_list


def create_ap_exam_results(file_record_template):
    file_list = []
    filename_template = 'ap_certified_{}-{}.csv'
    zip_filename_template = 'ap_certified_{}-{}.zip'
    topic = 'ap_exam_results'
    for year in range(2011, 2017):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://apps2.dpi.wi.gov/sdpr/download/files?topic='
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str"
            }
        }
        file_list.append(file_record)
    return file_list


def create_school_directory():
    wisedash_directory_record = {
        'root_download_url': 'https://dpi.wi.gov/sites/default/files/wise/downloads/',
        'filename': 'agency_current_all_years.csv',
        'file_format': 'csv',
        'topic': 'school_directory',
        'zip_import_parameters': {
            'zip_filename': 'agency_current_all_years.zip',
            'zip_file_path': ""
        },
        "pandas_dataframe_import_paramameters": {
            "na_values": ["Unknown"],
            "dtype": {
                "CESA": "str",
                "COUNTY_CODE": "str",
                "ATHLETIC_CONFERENCE_CODE": "str",
                "LOCALE_CODE": "str",
                "FULL_NCES_CODE": "str",
                "DISTRICT_CODE": "str",
                "SCHOOL_CODE": "str"
            }
        }
    }
    return [wisedash_directory_record]

def create_staff_teacher_experience(file_record_template):
    file_list = []
    filename_template = 'staff_teacher_experience_certified_{}-{}.csv'
    zip_filename_template = 'school-year={}-{}'
    topic = 'teacher_experience'
    for year in range(2007, 2014):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://apps2.dpi.wi.gov/sdpr/download/files?topic=staff-teacher-experience&'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA", "n/a"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str",
                "COUNTY": "str"
            }
        }
        file_list.append(file_record)
    return file_list 



def create_staff_teacher_education(file_record_template):
    file_list = []
    filename_template = 'staff_teacher_highest_degree_certified_{}-{}.csv'
    zip_filename_template = 'school-year={}-{}'
    topic = 'teacher_education'
    for year in range(2007, 2013):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://apps2.dpi.wi.gov/sdpr/download/files?topic==staff-teacher-highest-degree&'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA", "n/a"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str",
                "COUNTY": "str"
            }
        }
        file_list.append(file_record)
    return file_list 


def create_extracurricular_activities(file_record_template):
    file_list = []
    filename_template = 'extra_curricular_activities_certified_{}-{}.csv'
    zip_filename_template = 'school-year={}-{}'
    topic = 'extracurricular_activities'
    for year in range(2007, 2014):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://apps2.dpi.wi.gov/sdpr/download/files?topic=extra-curricular-activities&'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA", "n/a"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str",
                "COUNTY": "str"
            }
        }
        file_list.append(file_record)
    return file_list


def create_attendance_and_dropouts(file_record_template):
    file_list = []
    for filetype in ['attendance_certified', 'dropouts_certified']:
        filename_template = filetype + '_{}-{}.csv'
        zip_filename_template = 'attendance_dropouts_certified_{}-{}.zip'
        topic = 'attendance'
        for year in range(2011, 2016):
            file_record = copy.deepcopy(file_record_template)
            file_record['root_download_url']= 'https://dpi.wi.gov/sites/default/files/wise/downloads/'
            file_record['filename'] = add_years_to_template(filename_template, year)
            file_record['file_format'] = 'csv'
            file_record['topic'] = topic
            file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
            file_record['zip_import_parameters']['zip_file_path'] = ''
            file_record['pandas_dataframe_import_paramameters'] = {
                "na_values": ["*", "@NA", "n/a"],
                "dtype": {
                    "SCHOOL_CODE": "str",
                    "DISTRICT_CODE": "str",
                    "CESA": "str"
                }
            }
            file_list.append(file_record)
    return file_list


def create_habitual_truancy(file_record_template):
    file_list = []
    filename_template = 'habitual_truancy_certified_{}-{}.csv'
    zip_filename_template = 'school-year={}-{}'
    topic = 'habitual_truancy'
    for year in range(2007, 2014):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://apps2.dpi.wi.gov/sdpr/download/files?topic=habitual-truancy&'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA", "n/a", "#######"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str",
                "COUNTY": "str"
            }
        }
        file_list.append(file_record)
    return file_list


def create_hs_completion(file_record_template):
    file_list = []
    filename_template = 'hs_completion_certified_{}-{}.csv'
    zip_filename_template = 'hs_completion_certified_{}-{}.zip'
    topic = 'hs_completion'
    for year in range(2009, 2016):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://dpi.wi.gov/sites/default/files/wise/downloads/'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA", "n/a"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str"
            }
        }
        file_list.append(file_record)
    return file_list


def create_act_statewide(file_record_template):
    file_list = []
    filename_template = 'act_statewide_certified_{}-{}.csv'
    zip_filename_template = 'act_statewide_certified_{}-{}.zip'
    topic = 'act_statewide'
    for year in range(2014, 2017):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://dpi.wi.gov/sites/default/files/wise/downloads/'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA", "n/a"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str"
            }
        }
        file_list.append(file_record)
    return file_list


def create_act_graduates(file_record_template):
    file_list = []
    filename_template = 'act_graduates_certified_{}-{}.csv'
    zip_filename_template = 'act_graduates_certified_{}-{}.zip'
    zip_file_path_template = 'act_graduates_certified_{}-{}/'
    topic = 'act_statewide'
    for year in range(2009, 2016):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://dpi.wi.gov/sites/default/files/wise/downloads/'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        if year >= 2014:
            file_record['zip_import_parameters']['zip_file_path'] = ''
        else:
            file_record['zip_import_parameters']['zip_file_path'] = add_years_to_template(zip_file_path_template, year)
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA", "n/a"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str"
            }
        }
        file_list.append(file_record)
    return file_list

def create_hs_completion(file_record_template):
    file_list = []
    filename_template = 'hs_completion_certified_{}-{}.csv'
    zip_filename_template = 'hs_completion_certified_{}-{}.zip'
    topic = 'hs_completion'
    for year in range(2009, 2016):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://dpi.wi.gov/sites/default/files/wise/downloads/'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA", "n/a"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str"
            }
        }
        file_list.append(file_record)
    return file_list


def create_postsecondary_enrollment(file_record_template):
    file_list = []
    filename_template = 'postsecondary_enrollment_current_{}-{}.csv'
    zip_filename_template = 'postsecondary_enrollment_current_{}-{}.zip'
    topic = 'postsecondary_enrollment'
    for year in range(2006, 2016):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://dpi.wi.gov/sites/default/files/wise/downloads/'
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA", "n/a"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str"
            }
        }
        file_list.append(file_record)
    return file_list


def create_retention(file_record_template):
    file_list = []
    filename_template = 'retention_certified_{}-{}.csv'
    zip_filename_template = '{}-{}'
    topic = 'retention'
    for year in range(2008, 2016):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url']= 'https://apps2.dpi.wi.gov/sdpr/download/files?topic=retention&school-year='
        file_record['filename'] = add_years_to_template(filename_template, year)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = add_years_to_template(zip_filename_template, year)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["*", "@NA", "n/a"],
            "dtype": {
                "SCHOOL_CODE": "str",
                "DISTRICT_CODE": "str",
                "CESA": "str"
            }
        }
        file_list.append(file_record)
    return file_list

"""
load data
"""
conn = Conn('eddata', 'raw_data')

# create source record
load.source_to_db(conn, source_data)
source_id = 1

## create records for these files:
# aspire_test_results = create_aspire_test_results(file_record_format_d)
# school_directory = create_school_directory()
# ap_exam_results = create_ap_exam_results(file_record_format_d)
# # private_enrollment = create_private_enrollment(file_record_format_d)
# public_enrollment = create_public_enrollment(file_record_format_d)
# teacher_experience = create_staff_teacher_experience(file_record_format_d)
# teacher_education = create_staff_teacher_education(file_record_format_d)
# extracurricular_activities = create_extracurricular_activities(file_record_format_d)
# attendance_and_dropouts = create_attendance_and_dropouts(file_record_format_d)
# habitual_truancy = create_habitual_truancy(file_record_format_d)
# hs_completion = create_hs_completion(file_record_format_d)
# act_graduates = create_act_graduates(file_record_format_d)
# act_statewide = create_act_statewide(file_record_format_d)
retention = create_retention(file_record_format_d)
postsecondary_enrollment = create_postsecondary_enrollment(file_record_format_d)

#upload these file records
#file_categories = [aspire_test_results, school_directory, ap_exam_results, public_enrollment, teacher_experience, teacher_education, extracurricular_activities, habitual_truancy, hs_completion, act_statewide, act_graduates, postsecondary_enrollment, retention]
file_categories = [retention, postsecondary_enrollment]
for row in file_categories:
    load.file_records_to_db(row, source_id)

# upload data from file list
#topics_to_populate = ['aspire_test_results', 'ap_exam_results', 'school_directory', 'public_enrollment', 'teacher_experience', 'teacher_education', 'extracurricular_activities', 'attendance', 'habitual_truancy', 'hs_completion', 'postsecondary_enrollment', 'rentention']
topics_to_populate = ['retention', 'postsecondary_enrollment']
file_records_q = """
    SELECT 
            s.source_name
            ,f.*
    FROM 
            raw_data.file_ft f
    JOIN
            raw_data.source_dm s
        ON s.source_id = f.source_id   
    WHERE f.source_id = 1
    AND f.topic IN {}
""".format(tuple(topics_to_populate))

file_records = conn.execute_raw_query(file_records_q, fmt='dict')
for rec in file_records:
    load.data_file_to_db(rec)



