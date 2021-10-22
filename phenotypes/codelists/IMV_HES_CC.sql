SELECT 
    person_id_deid, date,
    '03_IMV_treatment' as covid_phenotype,
    'HES CC' as source
FROM 
    HES_CC
WHERE 
    ARESSUPDAYS > 0