CREATE SEQUENCE raw_data.source_dm_sequence;   
CREATE TABLE raw_data.source_dm (
    source_id  INT DEFAULT nextval('raw_data.source_dm_sequence') NOT NULL
    ,source_name VARCHAR
    ,publisher VARCHAR
    ,home_url VARCHAR
    ,date_added TIMESTAMP
    ,CONSTRAINT source_dm_unique_constraint UNIQUE(source_name, home_url)
);

CREATE SEQUENCE raw_data.file_ft_sequence;   
CREATE TABLE raw_data.file_ft (
    file_id  INT DEFAULT nextval('raw_data.file_ft_sequence') NOT NULL
    ,filename VARCHAR
    ,file_format VARCHAR
    ,source_id INT
    ,topic VARCHAR
    ,root_download_url VARCHAR
    ,pandas_dataframe_import_paramameters JSON
    ,zip_import_parameters JSON
    ,date_added TIMESTAMP
    ,CONSTRAINT file_dm_unique_constraint UNIQUE(filename, source_id, topic)
);