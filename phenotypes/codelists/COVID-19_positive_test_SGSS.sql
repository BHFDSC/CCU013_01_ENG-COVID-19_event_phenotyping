
SELECT 
    person_id_deid, 
    date, 
    "01_Covid_positive_test" as covid_phenotype, 
    CASE WHEN REPORTING_LAB_ID = '840' THEN "pillar_2" ELSE "pillar_1" END as description,
    "SGSS" as source
FROM 
    sgss