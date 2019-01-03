INSERT INTO dw.school_ft (
    nces_school_id
    ,local_school_id
    ,local_agency_id
    ,school_name
    ,phone_number
    ,location_address
    ,location_city
    ,location_state
    ,location_zip
    ,location_county
    ,school_type
    ,current_status
    ,geography_type_id
    ,union_status
    ,latitude
    ,longitude
    ,congressional_district
    ,is_bie_school
    ,is_tas_eligible
    ,tas_provided
    ,is_swp_eligible
    ,swp_provided
    ,is_magnet
    ,is_charter
    ,is_shared_school
    ,is_virtual_school
    ,is_nslp_participant
    ,nslp_provision
    ,sea_primary_public_charter_id
    ,sea_secondary_public_charter_id

)
SELECT
        ncessch as nces_school_id
        ,seasch as local_school_id
        ,stid as local_agency_id 
        ,schnam as school_name
        ,phone as phone_number
        ,lstree as location_address
        ,lcity as location_city
        ,lstate as location_state
        ,lzip as location_zip
        ,coname as location_county
        ,type as school_type
        ,status as current_status
        ,ulocal as geography_type_id
        ,union_status
        ,latcod as latitude
        ,loncod as longitude
        ,cdcode as congressional_district
        ,CASE WHEN bies=1 THEN TRUE WHEN bies=2 THEN FALSE END as is_bie_school
        ,CASE WHEN titleistat IN (1, 2) THEN TRUE WHEN titleistat NOT IN (1, 2) AND titleistat IS NOT NULL THEN FALSE END as is_tas_eligible
        ,CASE WHEN titleistat IN (1, 3) THEN TRUE WHEN titleistat NOT IN (1, 3) AND titleistat IS NOT NULL THEN FALSE END as tas_provided
        ,CASE WHEN titleistat IN (3, 4, 5) THEN TRUE WHEN titleistat NOT IN (3, 4, 5) AND titleistat IS NOT NULL THEN FALSE END as is_swp_eligible
        ,CASE WHEN titleistat IN (5) THEN TRUE WHEN titleistat NOT IN (5) AND titleistat IS NOT NULL THEN FALSE END as swp_provided
        ,CASE  WHEN magnet = 1 THEN TRUE WHEN magnet = 2 THEN FALSE END as is_magnet
        ,CASE  WHEN chartr = 1 THEN TRUE WHEN chartr = 2 THEN FALSE END as is_charter
        ,CASE  WHEN shared = 1 THEN TRUE WHEN shared = 2 THEN FALSE END as is_shared_school
        ,CASE  WHEN virtualstat = '1' THEN TRUE WHEN virtualstat = '2' THEN FALSE END as is_virtual_school
        ,CASE WHEN nslpstatus IN ('NSLPWOPRO', 'NSLPPRO1', 'NSLPPRO2', 'NSLPPRO3', 'NSLPCEO') THEN TRUE WHEN nslpstatus ='NSLPNO' THEN FALSE END as is_nslp_participant
        ,CASE WHEN nslpstatus = 'NSLPPRO1' THEN '1' WHEN nslpstatus = 'NSLPPRO2'  THEN '2'  WHEN nslpstatus = 'NSLPPRO3' THEN '3' WHEN nslpstatus = 'NSLPWOPRO' THEN 'None' WHEN nslpstatus = 'NSLPCEO' THEN 'CEO' END AS nslp_provision
        ,chartauth1 sea_primary_public_charter_id
        ,chartauth2 sea_secondary_public_charter_id
FROM
        raw_data.nces_national_survey
ON CONFLICT DO NOTHING;