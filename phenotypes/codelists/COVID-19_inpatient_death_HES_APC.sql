SELECT 
  person_id_deid,
  DISDATE as date,
  "04_Covid_inpatient_death" as covid_phenotype,
  (case when 
      DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed'
  when 
      DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected' 
  Else '0' End) as covid_status,
  "HES APC" as source
FROM
  HES_APC
WHERE 
  (DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%") -- COVID-19 admission
AND (DISMETH = 4 -- died
      OR 
    DISDEST = 79) -- discharge destination not applicable, died or stillborn
AND 
    (DISDATE >= TO_DATE("20200123", "yyyyMMdd")) -- death after study start