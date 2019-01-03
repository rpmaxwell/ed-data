CREATE SEQUENCE staging.school_ft_sequence;
CREATE TABLE "staging"."school_ft" (
    "school_id" INT DEFAULT nextval('dw.enrollment_dm_sequence') NOT NULL,
    "nces_school_id" varchar,
    "local_school_id" varchar,
    "local_agency_id" varchar,
    "school_name" varchar,
    "phone_number" varchar,
    "location_address" varchar,
    "location_city" varchar,
    "location_state" varchar,
    "location_zip" float8,
    "location_county" varchar,
    "school_type" int4,
    "current_status" int4,
    "geography_type_id" float8,
    "latitude" float8,
    "longitude" float8,
    "congressional_district" float8,
    "is_bie_school" bool,
    "is_tas_eligible" bool,
    "tas_provided" bool,
    "is_swp_eligible" bool,
    "swp_provided" bool,
    "is_magnet" bool,
    "is_charter" bool,
    "is_shared_school" bool,
    "is_virtual_school" bool,
    "is_nslp_participant" bool,
    "nslp_provision" text,
    "sea_primary_public_charter_id" varchar,
    "sea_secondary_public_charter_id" varchar,
    CONSTRAINT unique_school UNIQUE (nces_school_id)
);



CREATE SEQUENCE staging.demographic_dm_sequence;
CREATE TABLE staging.demographic_dm (
    demographic_id INT DEFAULT nextval('staging.demographic_dm_sequence') NOT NULL
    ,raw_demographic_label VARCHAR
    ,standardized_demographic_label VARCHAR
    ,date_added TIMESTAMP
    ,CONSTRAINT demographic_dm_unique_constraint UNIQUE(raw_demographic_label, standardized_demographic_label)
);


CREATE SEQUENCE dw.enrollment_dm_sequence;
CREATE TABLE dw.enrollment_dm (
    enrollment_id INT DEFAULT nextval('dw.enrollment_dm_sequence') NOT NULL
    ,school_year VARCHAR
    ,school_id INT
    ,demographic_group VARCHAR
    ,demographic_value VARCHAR
    ,student_count INT
    ,source_id INT
    ,CONSTRAINT unique_demographics UNIQUE (school_year, school_id, demographic_group, demographic_value)
);


CREATE TABLE staging.school_type_dm (
    school_type_id INT
    ,school_type_code VARCHAR
    ,description VARCHAR
);


CREATE TABLE IF NOT EXISTS dw.staff_dm (
  staff_id INT NOT NULL
  ,school_id INT
  ,school_year VARCHAR
  ,full_time_equivalent_classroom_teacher_count INT
);