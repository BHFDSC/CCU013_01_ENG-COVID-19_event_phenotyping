SELECT 
    apc.person_id_deid, 
    cc.CCSTARTDATE as date,
    '03_ICU_admission' as covid_phenotype,
    'HES CC' as source
FROM 
    HES_APC as apc
INNER JOIN 
    HES_CC as cc
ON 
    cc.SUSRECID = apc.SUSRECID
WHERE 
    cc.BESTMATCH = 1
AND 
    (apc.DIAG_4_CONCAT LIKE '%U071%' 
    OR apc.DIAG_4_CONCAT LIKE '%U072%')