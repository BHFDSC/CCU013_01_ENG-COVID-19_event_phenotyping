# Overview of Phenotypes used in *Characterising COVID-19 related events in a nationwide electronic health record cohort of 55.9 million people in England*  
<br>  

## COVID-19 event phenotypes defined within this body of work  

1. `01_Covid_positive_test.csv`  
2. `01_GP_covid_diagnosis.csv`  
3. `02_Covid_admission.csv`  
4. `03_NIV_treatment.csv`  
5. `03_IMV_treatment.csv`  
6. `03_ICU_admission.csv`
7. `03_ECMO_treatment.csv`
8. `04_Fatal_with_covid_diagnosis.csv`
9. `04_Fatal_without_covid_diagnosis.csv`
10. `04_Covid_inpatient_death.csv`  

<br>  

All 10 of these COVID-19 event phenotypes are included within `0_master_COVID19_event_phenotypes.csv`  
<br>  

## Other phenotypes used within this work  

1. `long-COVID` based on an aggregation across the following three [OpenSAFELY codelists](https://www.opencodelists.org/) these are best accessed on the [OpenSAFELY website](https://www.opencodelists.org/) however codelists and version ids used are committed to this repository for posterity:
    1. `opensafely-assessment-instruments-and-outcome-measures-for-long-covid` @ [OpenCodelists](https://www.opencodelists.org/codelist/opensafely/assessment-instruments-and-outcome-measures-for-long-covid/)  
    2.  `opensafely-nice-managing-the-long-term-effects-of-covid-19` @ [OpenCodelists](https://www.opencodelists.org/codelist/opensafely/nice-managing-the-long-term-effects-of-covid-19/)  
    3. `opensafely-referral-and-signposting-for-long-covid` @ [OpenCodelists](https://www.opencodelists.org/codelist/opensafely/referral-and-signposting-for-long-covid/)  
2. `high_risk` produced following [NHS-Digital Guidance on 'How to flag patients as high risk'](https://digital.nhs.uk/coronavirus/shielded-patient-list/guidance-for-general-practice#how-to-flag-patients-as-high-risk)  
<br>  


# COVID-19 event phenotype codelists

| **covid_phenotype**              | **clinical_code** | **terminology** | **description**                                                                                                                                       | **covid_status** | **source** |
|----------------------------------|-------------------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|------------|
| 01_Covid_positive_test           |                   |                 | pillar_2                                                                                                                                              | confirmed        | SGSS       |
| 01_Covid_positive_test           |                   |                 | pillar_1                                                                                                                                              | confirmed        | SGSS       |
| 01_GP_covid_diagnosis            | 1008541000000105  | SNOMED          | Coronavirus ribonucleic acid detection assay (observable entity)                                                                                      | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1029481000000103  | SNOMED          | Coronavirus nucleic acid detection assay (observable entity)                                                                                          | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 120814005         | SNOMED          | Coronavirus antibody (substance)                                                                                                                      | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240381000000105  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 (organism)                                                                                            | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240391000000107  | SNOMED          | Antigen of severe acute respiratory syndrome coronavirus 2 (substance)                                                                                | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240401000000105  | SNOMED          | Antibody to severe acute respiratory syndrome coronavirus 2 (substance)                                                                               | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240411000000107  | SNOMED          | Ribonucleic acid of severe acute respiratory syndrome coronavirus 2 (substance)                                                                       | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240421000000101  | SNOMED          | Serotype severe acute respiratory syndrome coronavirus 2 (qualifier value)                                                                            | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240511000000106  | SNOMED          | Detection of severe acute respiratory syndrome coronavirus 2 using polymerase chain reaction technique (procedure)                                    | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240521000000100  | SNOMED          | Otitis media caused by severe acute respiratory syndrome coronavirus 2 (disorder)                                                                     | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240531000000103  | SNOMED          | Myocarditis caused by severe acute respiratory syndrome coronavirus 2 (disorder)                                                                      | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240541000000107  | SNOMED          | Infection of upper respiratory tract caused by severe acute respiratory syndrome coronavirus 2 (disorder)                                             | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240551000000105  | SNOMED          | Pneumonia caused by severe acute respiratory syndrome coronavirus 2 (disorder)                                                                        | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240561000000108  | SNOMED          | Encephalopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)                                                                   | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240571000000101  | SNOMED          | Gastroenteritis caused by severe acute respiratory syndrome coronavirus 2 (disorder)                                                                  | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240581000000104  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)                                                                   | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240741000000103  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 serology (observable entity)                                                                          | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1240751000000100  | SNOMED          | Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 (disorder)                                                           | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1300631000000101  | SNOMED          | Coronavirus disease 19 severity score (observable entity)                                                                                             | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1300671000000104  | SNOMED          | Coronavirus disease 19 severity scale (assessment scale)                                                                                              | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1300681000000102  | SNOMED          | Assessment using coronavirus disease 19 severity scale (procedure)                                                                                    | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1300721000000109  | SNOMED          | Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed by laboratory test (situation)                             | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1300731000000106  | SNOMED          | Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed using clinical diagnostic criteria (situation)             | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321181000000108  | SNOMED          | Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 record extraction simple reference set (foundation metadata concept) | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321191000000105  | SNOMED          | Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 procedures simple reference set (foundation metadata concept)        | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321201000000107  | SNOMED          | Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 health issues simple reference set (foundation metadata concept)     | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321241000000105  | SNOMED          | Cardiomyopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)                                                                   | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321301000000101  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 ribonucleic acid qualitative existence in specimen (observable entity)                                | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321311000000104  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 immunoglobulin M qualitative existence in specimen (observable entity)                                | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321321000000105  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 immunoglobulin G qualitative existence in specimen (observable entity)                                | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321331000000107  | SNOMED          | Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 total immunoglobulin in serum (observable entity)                          | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321341000000103  | SNOMED          | Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin G in serum (observable entity)                              | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321351000000100  | SNOMED          | Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin M in serum (observable entity)                              | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321541000000108  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 immunoglobulin G detected (finding)                                                                   | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321551000000106  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 immunoglobulin M detected (finding)                                                                   | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321761000000103  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 immunoglobulin A detected (finding)                                                                   | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321801000000108  | SNOMED          | Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin A in serum (observable entity)                              | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1321811000000105  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 immunoglobulin A qualitative existence in specimen (observable entity)                                | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1322781000000102  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)                                                           | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 1322871000000109  | SNOMED          | Severe acute respiratory syndrome coronavirus 2 antibody detection result positive (finding)                                                          | confirmed        | GDPPR      |
| 01_GP_covid_diagnosis            | 186747009         | SNOMED          | Coronavirus infection (disorder)                                                                                                                      | confirmed        | GDPPR      |
| 02_Covid_admission               |                   |                 | HospitalAdmissionDate IS NOT null                                                                                                                     | confirmed        | CHESS      |
| 02_Covid_admission               | U07.1             | ICD10           | Confirmed_COVID19                                                                                                                                     | confirmed        | HES APC    |
| 02_Covid_admission               | U07.2             | ICD10           | Suspected_COVID19                                                                                                                                     | suspected        | HES APC    |
| 02_Covid_admission               | U07.1             | ICD10           | Confirmed_COVID19                                                                                                                                     | confirmed        | SUS        |
| 02_Covid_admission               | U07.2             | ICD10           | Suspected_COVID19                                                                                                                                     | suspected        | SUS        |
| 03_ECMO_treatment                |                   |                 | RespiratorySupportECMO == Yes                                                                                                                         | confirmed        | CHESS      |
| 03_ECMO_treatment                | X58.1             | OPCS            | Extracorporeal membrane oxygenation                                                                                                                   | confirmed        | HES APC    |
| 03_ECMO_treatment                | X58.1             | OPCS            | Extracorporeal membrane oxygenation                                                                                                                   | confirmed        | SUS        |
| 03_ICU_admission                 |                   |                 | DateAdmittedICU IS NOT null                                                                                                                           | confirmed        | CHESS      |
| 03_ICU_admission                 |                   |                 | id is in hes_cc table                                                                                                                                 | confirmed        | HES CC     |
| 03_IMV_treatment                 |                   |                 | Invasivemechanicalventilation == Yes                                                                                                                  | confirmed        | CHESS      |
| 03_IMV_treatment                 | E85.1             | OPCS            | Invasive ventilation                                                                                                                                  | confirmed        | HES APC    |
| 03_IMV_treatment                 | X56               | OPCS            | Intubation of trachea                                                                                                                                 | confirmed        | HES APC    |
| 03_IMV_treatment                 |                   |                 | ARESSUPDAYS > 0                                                                                                                                       | confirmed        | HES CC     |
| 03_IMV_treatment                 | E85.1             | OPCS            | Invasive ventilation                                                                                                                                  | confirmed        | SUS        |
| 03_IMV_treatment                 | X56               | OPCS            | Intubation of trachea                                                                                                                                 | confirmed        | SUS        |
| 03_NIV_treatment                 |                   |                 | Highflownasaloxygen OR NoninvasiveMechanicalventilation == Yes                                                                                        | confirmed        | CHESS      |
| 03_NIV_treatment                 | E85.2             | OPCS            | Non-invasive ventilation NEC                                                                                                                          | confirmed        | HES APC    |
| 03_NIV_treatment                 | E85.6             | OPCS            | Continuous positive airway pressure                                                                                                                   | confirmed        | HES APC    |
| 03_NIV_treatment                 |                   |                 | bressupdays > 0                                                                                                                                       | confirmed        | HES CC     |
| 03_NIV_treatment                 | E85.2             | OPCS            | Non-invasive ventilation NEC                                                                                                                          | confirmed        | SUS        |
| 03_NIV_treatment                 | E85.6             | OPCS            | Continuous positive airway pressure                                                                                                                   | confirmed        | SUS        |
| 04_Covid_inpatient_death         |                   |                 | DISMETH = 4 (Died)                                                                                                                                    | suspected        | HES APC    |
| 04_Covid_inpatient_death         |                   |                 | DISDEST = 79 (Not applicable - PATIENT died or still birth)                                                                                           | confirmed        | HES APC    |
| 04_Covid_inpatient_death         |                   |                 | DISMETH = 4 (Died)                                                                                                                                    | confirmed        | HES APC    |
| 04_Covid_inpatient_death         |                   |                 | DISDEST = 79 (Not applicable - PATIENT died or still birth)                                                                                           | suspected        | HES APC    |
| 04_Covid_inpatient_death         |                   |                 | DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79 (Not applicable - PATIENT died or still birth)                                                     | confirmed        | SUS        |
| 04_Covid_inpatient_death         |                   |                 | DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 (Died)                                                                                                   | suspected        | SUS        |
| 04_Covid_inpatient_death         |                   |                 | DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79 (Not applicable - PATIENT died or still birth)                                                     | suspected        | SUS        |
| 04_Covid_inpatient_death         |                   |                 | DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 (Died)                                                                                                   | confirmed        | SUS        |
| 04_Fatal_with_covid_diagnosis    | U071              | ICD10           |                                                                                                                                                       | confirmed        | deaths     |
| 04_Fatal_with_covid_diagnosis    | U072              | ICD10           |                                                                                                                                                       | suspected        | deaths     |
| 04_Fatal_without_covid_diagnosis |                   |                 | ONS death within 28 days                                                                                                                              | suspected        | deaths     |  
  
<br>  

# Other phenotype codelists
## 1. `long_covid` phenotype codelists  
  
  
| **phenotype** | **code**         | **term**                                                                              | **terminology** | **organisation** | **codelist_id**                                                       | **id**   | **version** |
|---------------|------------------|---------------------------------------------------------------------------------------|-----------------|------------------|-----------------------------------------------------------------------|----------|-------------|
| long_covid    | 1325031000000108 | Referral to post-COVID assessment clinic                                              | SNOMED CT       | OpenSAFELY       | opensafely/referral-and-signposting-for-long-covid                    | 12d06dc0 | 12d06dc0    |
| long_covid    | 1325051000000101 | Newcastle post-COVID syndrome Follow-up Screening Questionnaire                       | SNOMED CT       | OpenSAFELY       | opensafely/assessment-instruments-and-outcome-measures-for-long-covid | 79c0fa8a | 79c0fa8a    |
| long_covid    | 1325081000000107 | Assessment using COVID-19 Yorkshire Rehabilitation Screening tool                     | SNOMED CT       | OpenSAFELY       | opensafely/assessment-instruments-and-outcome-measures-for-long-covid | 79c0fa8a | 79c0fa8a    |
| long_covid    | 1325161000000102 | Post-COVID-19 syndrome                                                                | SNOMED CT       | OpenSAFELY       | opensafely/nice-managing-the-long-term-effects-of-covid-19            | 64f1ae69 | 64f1ae69    |
| long_covid    | 1325041000000104 | Referral to Your COVID Recovery rehabilitation platform                               | SNOMED CT       | OpenSAFELY       | opensafely/referral-and-signposting-for-long-covid                    | 12d06dc0 | 12d06dc0    |
| long_covid    | 1325101000000101 | Assessment using Post-COVID-19 Functional Status Scale patient self-report            | SNOMED CT       | OpenSAFELY       | opensafely/assessment-instruments-and-outcome-measures-for-long-covid | 79c0fa8a | 79c0fa8a    |
| long_covid    | 1325121000000105 | Post-COVID-19 Functional Status Scale patient self-report final scale grade           | SNOMED CT       | OpenSAFELY       | opensafely/assessment-instruments-and-outcome-measures-for-long-covid | 79c0fa8a | 79c0fa8a    |
| long_covid    | 1325141000000103 | Assessment using Post-COVID-19 Functional Status Scale structured interview           | SNOMED CT       | OpenSAFELY       | opensafely/assessment-instruments-and-outcome-measures-for-long-covid | 79c0fa8a | 79c0fa8a    |
| long_covid    | 1325151000000100 | Post-COVID-19 Functional Status Scale structured interview                            | SNOMED CT       | OpenSAFELY       | opensafely/assessment-instruments-and-outcome-measures-for-long-covid | 79c0fa8a | 79c0fa8a    |
| long_covid    | 1325021000000106 | Signposting to Your COVID Recovery                                                    | SNOMED CT       | OpenSAFELY       | opensafely/referral-and-signposting-for-long-covid                    | 12d06dc0 | 12d06dc0    |
| long_covid    | 1325181000000106 | Ongoing symptomatic disease caused by severe acute respiratory syndrome coronavirus 2 | SNOMED CT       | OpenSAFELY       | opensafely/nice-managing-the-long-term-effects-of-covid-19            | 64f1ae69 | 64f1ae69    |
| long_covid    | 1325071000000105 | COVID-19 Yorkshire Rehabilitation Screening tool                                      | SNOMED CT       | OpenSAFELY       | opensafely/assessment-instruments-and-outcome-measures-for-long-covid | 79c0fa8a | 79c0fa8a    |
| long_covid    | 1325061000000103 | Assessment using Newcastle post-COVID syndrome Follow-up Screening Questionnaire      | SNOMED CT       | OpenSAFELY       | opensafely/assessment-instruments-and-outcome-measures-for-long-covid | 79c0fa8a | 79c0fa8a    |
| long_covid    | 1325131000000107 | Post-COVID-19 Functional Status Scale structured interview final scale grade          | SNOMED CT       | OpenSAFELY       | opensafely/assessment-instruments-and-outcome-measures-for-long-covid | 79c0fa8a | 79c0fa8a    |
| long_covid    | 1325091000000109 | Post-COVID-19 Functional Status Scale patient self-report                             | SNOMED CT       | OpenSAFELY       | opensafely/assessment-instruments-and-outcome-measures-for-long-covid | 79c0fa8a | 79c0fa8a    |  
  
## 2. `high_risk` phenotype codelists  
| **code**         | **term**                                                                                                                                       | **terminology** | **organisation** |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|------------------|
| 1300561000000107 | High risk category for developing complication from coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 infection | SNOMED CT       | NHS Digital      |