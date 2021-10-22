SELECT 
    person_id_deid, 
    HospitalAdmissionDate as date,
    "02_Covid_admission" as covid_phenotype,
    "CHESS" as source
FROM 
    CHESS
WHERE 
    HospitalAdmissionDate IS NOT null