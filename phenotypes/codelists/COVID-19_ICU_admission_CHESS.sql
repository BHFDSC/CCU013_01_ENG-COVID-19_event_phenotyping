SELECT 
    person_id_deid, 
    DateAdmittedICU as date,
    "03_ICU_admission" as covid_phenotype,
    "CHESS" as source
FROM 
    CHESS
WHERE 
    DateAdmittedICU IS NOT null