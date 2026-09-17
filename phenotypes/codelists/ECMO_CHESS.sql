SELECT 
    person_id_deid, 
    DateAdmittedICU as date, -- Can't be any more precise, reasonable assumption ECMO in ICU
    "03_ECMO_treatment" as covid_phenotype,
    "CHESS" as source
FROM 
    CHESS
WHERE 
    HospitalAdmissionDate IS NOT null
AND 
    RespiratorySupportECMO == "Yes"