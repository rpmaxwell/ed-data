    INSERT INTO staging.school_type_dm (
	school_type_id
	,school_type_code
	,description
)
(
SELECT 
	DISTINCT nns."type" as school_type_id
	,CASE WHEN nns."type" = 1 THEN 'regular'
		  WHEN nns."type" = 2 THEN 'special_education'
		  WHEN nns."type" = 3 THEN 'vocational'
		  WHEN nns."type" = 4 THEN 'alternative'
		  WHEN nns."type" = 5 THEN 'reportable'
		  ELSE NULL END as school_type_code
	,CASE WHEN nns."type" = 1 THEN 'Regular school'
		  WHEN nns."type" = 2 THEN 'Special education school'
		  WHEN nns."type" = 3 THEN 'Vocational school'
		  WHEN nns."type" = 4 THEN 'Alternative school'
		  WHEN nns."type" = 5 THEN 'Reportable program'
		  ELSE NULL END as description
FROM
	raw_data.nces_national_survey nns
);