SELECT 
    person_id_deid, 
    HospitalAdmissionDate as date, -- Can't be any more precise
    "03_NIV_treatment" as covid_phenotype,
    "CHESS" as source
FROM 
    CHESS
WHERE 
    HospitalAdmissionDate IS NOT null
AND 
    (Highflownasaloxygen == "Yes" 
    OR 
    NoninvasiveMechanicalventilation == "Yes")