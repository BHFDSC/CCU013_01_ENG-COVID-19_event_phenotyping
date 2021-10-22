SELECT 
    person_id_deid, date,
    '03_NIV_treatment' as covid_phenotype,
    'HES CC' as source
FROM 
    HES_CC
WHERE 
    BRESSUPDAYS > 0