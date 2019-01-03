import copy
import json
import os
from dconn import Conn
import load
from psycopg2.extras import Json

with open(os.environ['FILE_RECORD_TEMPLATE']) as f:
    file_record_format_d = json.loads(f.read())



source_data = {
    "home_url": "http://www.p12.nysed.gov/",
    "source_name": "nysed",
    "publisher": "New York State Education Department"
}

topic_template_map = {
	'students_with_disabilities': 'PublicSchool{}SWD.xlsx',
 	'english_language_proficency': 'PublicSchool{}LEP.xlsx',
 	'economic_status': 'PublicSchool{}EconDisadv.xlsx',
 	'all_students': 'PublicSchool{}AllStudents.xlsx',
 	'race': 'PublicSchool{}Race.xlsx',
 	'gender': 'PublicSchool{}Gender.xlsx'
}


def create_school_enrollment(file_record_template):
	topic_template_map = {
		'students_with_disabilities': 'PublicSchool{}SWD.xlsx',
	 	'english_language_proficency': 'PublicSchool{}LEP.xlsx',
	 	'economic_status': 'PublicSchool{}EconDisadv.xlsx',
	 	'all_students': 'PublicSchool{}AllStudents.xlsx',
	 	'race': 'PublicSchool{}Race.xlsx',
	 	'gender': 'PublicSchool{}Gender.xlsx'
	}
	file_list = []
	for topic in topic_template_map.keys():
		filename_template = topic_template_map[topic]
		for year in range(2012, 2018):
			if (year == 2013):
				continue
			file_record = copy.deepcopy(file_record_template)
			file_record['root_download_url'] = 'http://www.p12.nysed.gov/irs/statistics/enroll-n-staff/'
			file_record['filename'] = filename_template.format(year)
			file_record['file_format'] = 'excel'
			file_record['topic'] = 'public_enrollment'
			file_record['pandas_dataframe_import_paramameters'] = {
				"na_values": ["-"],
				"converters": {
					"PK12 Total":"int",
					"STATE LOCATION ID": "str",
					"STATE DISTRICT ID": "str",
					"SUBGROUP CODE": "str"
				}
			}
			file_record['zip_import_parameters'] = None
			file_list.append(file_record)
	return file_list


def create_graduation_rates(file_record_template):
    file_list = []
    filename_template = 'GRAD_RATE_AND_OUTCOMES_{}.csv'
    zip_filename_template = 'gradrate_{}.zip'
    root_download_url_template = 'https://data.nysed.gov/files/gradrate/{}-{}/'
    topic = 'graduation_rates'
    for year in range(2014, 2016):
        file_record = copy.deepcopy(file_record_template)
        file_record['root_download_url'] = root_download_url_template.format(str(year)[2:], str(year+1)[2:])
        file_record['filename'] = filename_template.format(year+1)
        file_record['file_format'] = 'csv'
        file_record['topic'] = topic
        file_record['zip_import_parameters']['zip_filename'] = zip_filename_template.format(year+1)
        file_record['zip_import_parameters']['zip_file_path'] = ''
        file_record['pandas_dataframe_import_paramameters'] = {
            "na_values": ["-"],
            "dtype": {
                'AGGREGATION_CODE': 'str',
                'LEA_BEDS': 'str',
                'MEMBERSHIP_CODE': 'str',
                'BOCES_CODE': 'str',
                'NRC_CODE': 'str',
                'ENROLL_CNT': 'int',
                'ENTITY_INACTIVE_DATE': 'int',
                'COUNTY_CODE': 'int',
                'NYC_IND': 'int',
                'GRAD_CNT': 'int',
                'LOCAL_CNT': 'int',
                'REG_CNT': 'int',
                'REG_ADV_CNT': 'int',
                'NON_DIPLOMA_CREDENTIAL_CNT': 'int',
                'STILL_ENR_CNT': 'int',
                'GED_CNT': 'int',
                'DROPOUT_CNT': 'int'
            }
        }
        file_list.append(file_record)
    return file_list

def create_school_directory():
    file_record =  [{
        'root_download_url': 'http://www.p12.nysed.gov/irs/schoolDirectory/',
        'filename': '2016_12_02_SchoolDirectory.xlsx',
        'file_format': 'excel',
        'zip_import_parameters': None,
        'topic': 'school_directory',
        "pandas_dataframe_import_paramameters": {
            "na_values": ["Unknown"],
            "dtype": {
                "SED CODE": "str",
                "INSTITUTION TYPE CODE": "str",
                "INSTITUTION SUB TYPE CODE": "str",
                "NEEDS RESOURCE CODE": "str",
                "GRADE ORGANIZATION CODE": "str",
                "COMMUNITY TYPE CODE": "str",
                "COMMUNITY TYPE DESCRIPTION": "str",
                "ZIP": "str",
                "CEO PHONE": "str"
            }
        }
    }]
    return file_record


def remove_errant_column_from_grad_data(df):
    errant_col = 'ENTITY_INACTIVE_DATE'
    if errant_col in df.columns:
        df.drop(errant_col, axis=1, inplace=True)
    return df


"""
load data
"""
conn = Conn('eddata', 'raw_data')

# ## create source record
load.source_to_db(conn, source_data)
source_id = 2

## create records for these files:
school_directory = create_school_directory()
school_enrollment = create_school_enrollment(file_record_format_d)
graduation_rates = create_graduation_rates(file_record_format_d)


# #upload these file records
file_categories = [school_directory, school_enrollment, graduation_rates]
for row in file_categories:
    load.file_records_to_db(row, source_id)

def format_graduation_cols(df):
    f = lambda x: int(x.strip('%'))
    pct_cols = ['grad_pct',
         'local_pct',
         'reg_pct',
         'reg_adv_pct',
         'non_diploma_credential_pct',
         'still_enr_pct',
         'ged_pct',
         'dropout_pct']
    for col in pct_cols:
        df[col] = df[col].apply(f)



topics_to_populate = ['graduation_rates', 'public_enrollment']

file_records_q = """
    SELECT
            s.source_name
            ,f.*
    FROM
            raw_data.file_ft f
    JOIN
            raw_data.source_dm s
        ON s.source_id = f.source_id
    WHERE s.source_id = 2
    AND f.topic IN {}
""".format(tuple(topics_to_populate))

file_records = conn.execute_raw_query(file_records_q, fmt='dict')
for rec in file_records[30:]:
    if rec['topic'] == 'graduation_rates':
        remove_bad_row_and_column = lambda x: remove_errant_column_from_grad_data(x.iloc[1:])
        load.data_file_to_db(rec, remove_bad_row_and_column)
    else:   
        load.data_file_to_db(rec, remove_errant_column_from_grad_data)

