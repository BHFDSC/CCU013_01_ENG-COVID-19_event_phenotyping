# Databricks notebook source
# MAGIC %md
# MAGIC # Build CALIBER codelist  
# MAGIC 
# MAGIC **Description**  
# MAGIC   
# MAGIC 1. Imports the CALIBER codelist dictionary from *Kuan V., Denaxas S., Gonzalez-Izquierdo A. et al. A chronological map of 308 physical and mental health conditions from 4 million individuals in the National Health Service published in the Lancet Digital Health - DOI 10.1016/S2589-7500(19)30012-3* at https://github.com/spiros/chronological-map-phenotypes  
# MAGIC   * NB owing to lack of external connectivity within the TRE, this is imported by hardcoding  
# MAGIC 2. Applies a series of 'regex' transformations to map the dictionary to the format of codelists stored within the TRE `bhf_cvd_covid_uk_byod` database  
# MAGIC 3. Uses the transformed dictionary to loop through all relevant codelists and compile a **master codelist** of: `phenotype`, `code` & `terminology`
# MAGIC 
# MAGIC 
# MAGIC **Project(s)** CCU013
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson
# MAGIC  
# MAGIC **Reviewer(s)** *UNREVIEWED !!!*
# MAGIC  
# MAGIC **Date last updated** 2021-04-22
# MAGIC  
# MAGIC **Date last reviewed** *UNREVIEWED !!!*
# MAGIC  
# MAGIC **Date last run** 2021-09-08
# MAGIC 
# MAGIC **Changelog**  
# MAGIC 0. `21-04-22` **Initial version**
# MAGIC 1. `21-07-13`:  
# MAGIC   a) upgrade to optimised delta table  
# MAGIC   b) sort phenotype regexp cleaning  
# MAGIC   
# MAGIC  
# MAGIC **Data input**  
# MAGIC * ALL `caliber_...` tables in `bhf_cvd_covid_uk_byod` database  
# MAGIC   
# MAGIC   
# MAGIC **Data output**
# MAGIC * `ccu013_caliber_dictionary`
# MAGIC * `ccu013_caliber_codelist_master`
# MAGIC   
# MAGIC **Software and versions** `python`, `sql`
# MAGIC  
# MAGIC **Packages and versions** `pyspark`, `pandas`

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU013/COVID-19-SEVERITY-PHENOTYPING/CCU013_00_helper_functions 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Exploration of phenotypes in `bhf_cvd_covid_uk_byod`

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bhf_cvd_covid_uk_byod

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bhf_cvd_covid_uk_byod LIKE "caliber*"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Terminologies
# MAGIC There is no column for this so have to infer from the table name
# MAGIC 
# MAGIC | Terminology    | **tableName** | **col** |  date field | 
# MAGIC | ----------- | ----------- |
# MAGIC | SNOMED   |  `caliber_cprd_***_snomedct` | `conceptId` |  `RecordDate` |
# MAGIC | Readcode   |  `caliber_cprd_***` | `Readcode` |  `RecordDate` |
# MAGIC | Medcode   |  `caliber_cprd_***`  | `Medcode` |  `RecordDate` |
# MAGIC | ICD10   | `caliber_icd_***` | `ICD10code` |  `RecordDate` |
# MAGIC | OPCS4   | `caliber_opcs_***` | `OPCS4code` |  `RecordDate` |
# MAGIC 
# MAGIC 
# MAGIC So each terminology can be uniquely identified by the column name!

# COMMAND ----------

# MAGIC %md
# MAGIC Plan:
# MAGIC 1. Get all CALIBER table names
# MAGIC 2. Loop through and extract:
# MAGIC     * `Disease` as `phenotype`
# MAGIC     * code
# MAGIC     * terminology
# MAGIC 3. at end of loop append to the temp view `ccu013_caliber_codelists`
# MAGIC 4. make permanent  

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Build CALIBER dictionary (of codelists!)
# MAGIC 
# MAGIC The dictionary is hardcoded below by copying and pasting from [https://github.com/spiros/chronological-map-phenotypes/blob/master/dictionary.csv]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Hardcode dictionary

# COMMAND ----------

dictionary =  """phenotype,CPRD,ICD,OPCS
Abdominal Hernia,CPRD_hernia_abdo.csv,ICD_hernia_abdo.csv,OPCS_hernia_abdo.csv
Abdominal aortic aneurysm,CPRD_AAA.csv,ICD_AAA.csv,
Acne,CPRD_acne.csv,ICD_acne.csv,
Actinic keratosis,CPRD_actinic_keratosis.csv,ICD_actinic_keratosis.csv,
Acute Kidney Injury,,ICD_AKI.csv,
Agranulocytosis,CPRD_agranulocytosis.csv,ICD_agranulocytosis.csv,
Alcohol Problems,CPRD_alc_problems.csv,ICD_alc_problems.csv,
Alcoholic liver disease,CPRD_liver_alc.csv,ICD_liver_alc.csv,
Allergic and chronic rhinitis,CPRD_allergic_rhinitis.csv,ICD_allergic_rhinitis.csv,
Alopecia areata,CPRD_alopecia_areata.csv,ICD_alopecia_areata.csv,
Anal fissure,CPRD_anal_fissure.csv,ICD_anal_fissure.csv,OPCS_anal_fissure.csv
Angiodysplasia of colon,CPRD_angiodysplasia_colon.csv,ICD_angiodysplasia_colon.csv,
Ankylosing spondylitis,CPRD_ank_spond.csv,ICD_ank_spond.csv,
Anorectal fistula,CPRD_anorectal_fistula.csv,ICD_anorectal_fistula.csv,OPCS_anorectal_fistula.csv
Anorectal prolapse,CPRD_anorectal_prolapse.csv,ICD_anorectal_prolapse.csv,OPCS_anorectal_prolapse.csv
Anorexia and bulimia nervosa,CPRD_eating_dz.csv,ICD_eating_dz.csv,
Anterior and Intermediate Uveitis,CPRD_ant_uveitis.csv,ICD_ant_uveitis.csv,
Anxiety disorders,CPRD_anxiety.csv,ICD_anxiety.csv,
Aplastic anaemias,CPRD_aplastic.csv,ICD_aplastic.csv,
Appendicitis,CPRD_appendicitis.csv,ICD_appendicitis.csv,OPCS_appendicitis.csv
Asbestosis,CPRD_asbestosis.csv,ICD_asbestosis.csv,
Aspiration pneumonitis,CPRD_aspiration_pneumo.csv,ICD_aspiration_pneumo.csv,
Asthma,CPRD_asthma.csv,ICD_asthma.csv,
Atrial fibrillation,CPRD_AF.csv,ICD_AF.csv,
"Atrioventricular block, complete",CPRD_av_block_3.csv,ICD_av_block_3.csv,
"Atrioventricular block, first degree",CPRD_av_block_1.csv,ICD_av_block_1.csv,
"Atrioventricular block, second degree",CPRD_av_block_2.csv,ICD_av_block_2.csv,
Autism and Asperger's syndrome,CPRD_autism.csv,ICD_autism.csv,
Autoimmune liver disease,CPRD_autoimm_liver.csv,ICD_autoimm_liver.csv,
Bacterial Diseases (excl TB),,ICD_bacterial.csv,
Bacterial sepsis of newborn,CPRD_sepsis_newborn.csv,ICD_sepsis_newborn.csv,
Barrett's oesophagus,CPRD_barretts.csv,ICD_barretts.csv,
Bell's palsy,CPRD_bells.csv,ICD_bells.csv,
Benign neoplasm and polyp of uterus,CPRD_benign_uterus.csv,ICD_benign_uterus.csv,
Benign neoplasm of brain and other parts of central nervous system,CPRD_benign_brain.csv,ICD_benign_brain.csv,
"Benign neoplasm of colon, rectum, anus and anal canal",CPRD_benign_colon.csv,ICD_benign_colon.csv,
Benign neoplasm of ovary,CPRD_benign_ovary.csv,ICD_benign_ovary.csv,
Benign neoplasm of stomach and duodenum,CPRD_benign_stomach.csv,ICD_benign_stomach.csv,
Bifascicular block,CPRD_bifasc_block.csv,ICD_bifasc_block.csv,
Bipolar affective disorder and mania,CPRD_BAD.csv,ICD_BAD.csv,
Bronchiectasis,CPRD_bronchiectasis.csv,ICD_bronchiectasis.csv,
COPD,CPRD_COPD.csv,ICD_COPD.csv,
Carcinoma in situ_cervical,CPRD_cin_cervical.csv,ICD_cin_cervical.csv,
Carpal tunnel syndrome,CPRD_carpal_tunnel.csv,ICD_carpal_tunnel.csv,OPCS_carpal_tunnel.csv
Cataract,CPRD_cataract.csv,ICD_cataract.csv,OPCS_cataract.csv
Cerebral Palsy,CPRD_cerebral_palsy.csv,ICD_cerebral_palsy.csv,
Cholangitis,CPRD_cholangitis.csv,ICD_cholangitis.csv,
Cholecystitis,CPRD_cholecystitis.csv,ICD_cholecystitis.csv,
Cholelithiasis,CPRD_cholelithiasis.csv,ICD_cholelithiasis.csv,
Chronic sinusitis,CPRD_sinusitis.csv,ICD_sinusitis.csv,
Chronic viral hepatitis,CPRD_chr_hep.csv,ICD_chr_hep.csv,
Coeliac disease,CPRD_coeliac.csv,ICD_coeliac.csv,
Collapsed vertebra,CPRD_collapsed_vert.csv,ICD_collapsed_vert.csv,OPCS_collapsed_vert.csv
Congenital malformations of cardiac septa,CPRD_congenital_septal.csv,ICD_congenital_septal.csv,OPCS_congenital_septal.csv
Coronary heart disease not otherwise specified,CPRD_CHD_NOS.csv,ICD_CHD_NOS.csv,
Crohn's disease,CPRD_crohns.csv,ICD_crohns.csv,
Cystic Fibrosis,CPRD_CF.csv,ICD_CF.csv,
"Delirium, not induced by alcohol and other psychoactive substances",CPRD_delirium.csv,ICD_delirium.csv,
Dementia,CPRD_dementia.csv,ICD_dementia.csv,
Depression,CPRD_depression.csv,ICD_depression.csv,
Dermatitis (atopc/contact/other/unspecified),CPRD_dermatitis.csv,ICD_dermatitis.csv,
Diabetes,CPRD_diabetes.csv,ICD_diabetes.csv,
Diabetic neurological complications,CPRD_dm_neuro.csv,ICD_dm_neuro.csv,
Diabetic ophthalmic complications,CPRD_diab_eye.csv,ICD_diab_eye.csv,
Diaphragmatic hernia,CPRD_hernia_diaphragm.csv,ICD_hernia_diaphragm.csv,OPCS_hernia_diaphragm.csv
Dilated cardiomyopathy,CPRD_dcm.csv,ICD_dcm.csv,
Disorders of autonomic nervous system,CPRD_autonomic_neuro.csv,ICD_autonomic_neuro.csv,
Diverticular disease of intestine (acute and chronic),CPRD_diverticuli.csv,ICD_diverticuli.csv,OPCS_diverticuli.csv
Down's syndrome,CPRD_downs.csv,ICD_downs.csv,
Dysmenorrhoea,CPRD_dysmenorrhoea.csv,ICD_dysmenorrhoea.csv,
Ear and Upper Respiratory Tract Infections,,ICD_ear_urti.csv,
Encephalitis,,ICD_enceph.csv,
End stage renal disease,CPRD_ESRD.csv,ICD_ESRD.csv,OPCS_ESRD.csv
Endometrial hyperplasia and hypertrophy,CPRD_endometrial_hyper.csv,ICD_endometrial_hyper.csv,
Endometriosis,CPRD_endometriosis.csv,ICD_endometriosis.csv,
Enteropathic arthropathy,CPRD_entero_arthro.csv,ICD_entero_arthro.csv,
Enthesopathies & synovial disorders,CPRD_enthesopathy.csv,ICD_enthesopathy.csv,
Epilepsy,CPRD_epilepsy.csv,ICD_epilepsy.csv,
Erectile dysfunction,CPRD_ED.csv,ICD_ED.csv,
Essential tremor,CPRD_essential_tremor.csv,ICD_essential_tremor.csv,
Eye infections,,ICD_eye.csv,
Fatty Liver,CPRD_fatty_liver.csv,ICD_fatty_liver.csv,
Female genital prolapse,CPRD_female_genital_prolapse.csv,ICD_female_genital_prolapse.csv,
Female infertility,CPRD_female_infertility.csv,ICD_female_infertility.csv,
Female pelvic inflammatory disease,,ICD_PID.csv,
Fibromatoses,CPRD_fibromatosis.csv,ICD_fibromatosis.csv,OPCS_fibromatosis.csv
Folate deficiency anaemia,CPRD_folatedef.csv,ICD_folatedef.csv,
Fracture of hip,CPRD_fracture_hip.csv,ICD_fracture_hip.csv,OPCS_fracture_hip.csv
Fracture of wrist,CPRD_fracture_wrist.csv,ICD_fracture_wrist.csv,
Gastritis and duodenitis,CPRD_gastritis_duodenitis.csv,ICD_gastritis_duodenitis.csv,
Gastro-oesophageal reflux disease,CPRD_GORD.csv,ICD_GORD.csv,OPCS_GORD.csv
Giant Cell arteritis,CPRD_GCA.csv,ICD_GCA.csv,
Glaucoma,CPRD_glaucoma.csv,ICD_glaucoma.csv,OPCS_glaucoma.csv
Glomerulonephritis,CPRD_GN.csv,ICD_GN.csv,
Gout,CPRD_gout.csv,ICD_gout.csv,
HIV,CPRD_hiv.csv,ICD_hiv.csv,
"Haemangioma, any site",CPRD_haemangioma.csv,ICD_haemangioma.csv,
Hearing loss,CPRD_deaf.csv,ICD_deaf.csv,
Heart failure,CPRD_hf.csv,ICD_hf.csv,
Hepatic failure,CPRD_liver_fail.csv,ICD_liver_fail.csv,
Hidradenitis suppurativa,CPRD_hidradenitis.csv,ICD_hidradenitis.csv,
High birth weight,CPRD_HBW.csv,ICD_HBW.csv,
Hodgkin Lymphoma,CPRD_hodgkins.csv,ICD_hodgkins.csv,
Hydrocoele (incl infected),CPRD_hydrocele.csv,ICD_hydrocele.csv,
Hyperkinetic disorders,CPRD_ADHD.csv,ICD_ADHD.csv,
Hyperparathyroidism,CPRD_PTH.csv,ICD_PTH.csv,
Hyperplasia of prostate,CPRD_BPH.csv,ICD_BPH.csv,
Hypertension,CPRD_hypertension.csv,ICD_hypertension.csv,
Hypertrophic Cardiomyopathy,CPRD_hocm.csv,ICD_hocm.csv,
Hypertrophy of nasal turbinates,CPRD_hyper_nasal_turbs.csv,ICD_hyper_nasal_turbs.csv,
Hypo or hyperthyroidism,CPRD_thyroid.csv,ICD_thyroid.csv,
Hyposplenism,CPRD_hyposplenism.csv,ICD_hyposplenism.csv,
Immunodeficiencies,CPRD_immunodef.csv,ICD_immunodef.csv,
Infection of anal and rectal regions,,ICD_anorectal.csv,
Infection of bones and joints,,ICD_bone.csv,
Infection of liver,,ICD_liver.csv,
Infection of male genital system,,ICD_male_GU.csv,
Infection of other or unspecified genitourinary system,,ICD_oth_gu.csv,
Infection of skin and subcutaneous tissues,,ICD_skin.csv,
Infections of Other or unspecified organs,,ICD_oth_organs.csv,
Infections of the Heart,,ICD_heart.csv,
Infections of the digestive system,,ICD_digestive.csv,
Intellectual disability,CPRD_intell_dz.csv,ICD_intell_dz.csv,
Intervertebral disc disorders,CPRD_intervert_disc.csv,ICD_intervert_disc.csv,OPCS_intervert_disc.csv
Intracerebral haemorrhage,CPRD_Intracereb_haem.csv,ICD_Intracereb_haem.csv,
Intracranial hypertension,CPRD_intracranial_htn.csv,ICD_intracranial_htn.csv,
Intrauterine hypoxia,CPRD_intrauterine_hypoxia.csv,ICD_intrauterine_hypoxia.csv,
Iron deficiency anaemia,CPRD_IDA.csv,ICD_IDA.csv,
Irritable bowel syndrome,CPRD_IBS.csv,ICD_IBS.csv,
Ischaemic stroke,CPRD_Isch_stroke.csv,ICD_Isch_stroke.csv,
Juvenile arthritis,CPRD_juv_arth.csv,ICD_juv_arth.csv,
Keratitis,CPRD_keratitis.csv,ICD_keratitis.csv,
Left bundle branch block,CPRD_LBBB.csv,ICD_LBBB.csv,
Leiomyoma of uterus,CPRD_leiomyoma.csv,ICD_leiomyoma.csv,
Leukaemia,CPRD_leukaemia.csv,ICD_leukaemia.csv,
Lichen planus,CPRD_lichen_planus.csv,ICD_lichen_planus.csv,
"Liver fibrosis, sclerosis and cirrhosis",CPRD_cirrhosis.csv,ICD_cirrhosis.csv,
Lower Respiratory Tract Infections,,ICD_lrti.csv,
Lupus erythematosus (local and systemic),CPRD_SLE.csv,ICD_SLE.csv,
Macular degeneration,CPRD_macula_degen.csv,ICD_macula_degen.csv,
Male infertility,CPRD_male_infertility.csv,ICD_male_infertility.csv,
Meniere disease,CPRD_meniere.csv,ICD_meniere.csv,
Meningitis,,ICD_meningitis.csv,
Menorrhagia and polymenorrhoea,CPRD_menorrhagia.csv,ICD_menorrhagia.csv,
Migraine,CPRD_migraine.csv,ICD_migraine.csv,
Monoclonal gammopathy of undetermined significance (MGUS),CPRD_MGUS.csv,ICD_MGUS.csv,
Motor neuron disease,CPRD_MND.csv,ICD_MND.csv,
Multiple myeloma and malignant plasma cell neoplasms,CPRD_plasmacell.csv,ICD_plasmacell.csv,
Multiple sclerosis,CPRD_MS.csv,ICD_MS.csv,
Multiple valve dz,CPRD_mult_valve.csv,ICD_mult_valve.csv,
Myasthenia gravis,CPRD_myasthenia.csv,ICD_myasthenia.csv,
Mycoses,,ICD_mycoses.csv,
Myelodysplastic syndromes,CPRD_MDS.csv,ICD_MDS.csv,
Myocardial infarction,CPRD_myocardial_infarction.csv,ICD_myocardial_infarction.csv,
Nasal polyp,CPRD_nasal_polyp.csv,ICD_nasal_polyp.csv,
Neonatal jaundice (excl haemolytic dz of the newborn),CPRD_neo_jaundice.csv,ICD_neo_jaundice.csv,
Neuromuscular dysfunction of bladder,CPRD_neuro_bladder.csv,ICD_neuro_bladder.csv,
Non-Hodgkin Lymphoma,CPRD_NHL.csv,ICD_NHL.csv,
Non-acute cystitis,CPRD_chr_cystitis.csv,ICD_chr_cystitis.csv,
Nonrheumatic aortic valve disorders,CPRD_nonRh_aortic.csv,ICD_nonRh_aortic.csv,
Nonrheumatic mitral valve disorders,CPRD_nonRh_mitral.csv,ICD_nonRh_mitral.csv,
Obesity,CPRD_obesity.csv,ICD_obesity.csv,
Obsessive-compulsive disorder,CPRD_ocd.csv,ICD_ocd.csv,
Obstructive and reflux uropathy,CPRD_obstr_reflux.csv,ICD_obstr_reflux.csv,
Oesophageal varices,CPRD_varices.csv,ICD_varices.csv,OPCS_varices.csv
Oesophagitis and oesophageal ulcer,CPRD_oesoph_ulc.csv,ICD_oesoph_ulc.csv,
Osteoarthritis (excl spine),CPRD_OA.csv,ICD_OA.csv,
Osteoporosis,CPRD_osteoporosis.csv,ICD_osteoporosis.csv,
Other Cardiomyopathy,CPRD_cardiomyo_oth.csv,ICD_cardiomyo_oth.csv,
Other anaemias,CPRD_oth_anaemia.csv,ICD_oth_anaemia.csv,
Other haemolytic anaemias,CPRD_oth_haem_anaemia.csv,ICD_oth_haem_anaemia.csv,
Other interstitial pulmonary diseases with fibrosis,CPRD_pulm_fibrosis.csv,ICD_pulm_fibrosis.csv,
Other nervous system infections,,ICD_oth_nerv_sys.csv,
Other or unspecified infectious organisms,,ICD_oth_organisms.csv,
Other psychoactive substance misuse,CPRD_substance_misuse.csv,ICD_substance_misuse.csv,
Pancreatitis,CPRD_pancreatitis.csv,ICD_pancreatitis.csv,
Parasitic infections,,ICD_parasitic.csv,
Parkinson's disease,CPRD_Parkinsons.csv,ICD_Parkinsons.csv,
Patent ductus arteriosus,CPRD_PDA.csv,ICD_PDA.csv,OPCS_PDA.csv
Peptic ulcer disease,CPRD_ulcer_peptic.csv,ICD_ulcer_peptic.csv,OPCS_ulcer_peptic.csv
Pericardial effusion (noninflammatory),CPRD_pericardial_effusion.csv,ICD_pericardial_effusion.csv,
Peripheral arterial disease,CPRD_peripheral_arterial_disease.csv,ICD_peripheral_arterial_disease.csv,OPCS_peripheral_arterial_disease.csv
Peripheral neuropathies (excluding cranial nerve and carpal tunnel syndromes),CPRD_periph_neuro.csv,ICD_periph_neuro.csv,
Peritonitis,CPRD_peritonitis.csv,ICD_peritonitis.csv,OPCS_peritonitis.csv
Personality disorders,CPRD_PD.csv,ICD_PD.csv,
Pilonidal cyst/sinus,CPRD_pilonidal.csv,ICD_pilonidal.csv,OPCS_pilonidal.csv
Pleural effusion,CPRD_pleural_effusion.csv,ICD_pleural_effusion.csv,
Pleural plaque,CPRD_pleural_plaque.csv,ICD_pleural_plaque.csv,
Pneumothorax,CPRD_pneumothorax.csv,ICD_pneumothorax.csv,
Polycystic ovarian syndrome,CPRD_PCOS.csv,ICD_PCOS.csv,
Polycythaemia vera,CPRD_PCV.csv,ICD_PCV.csv,
Polymyalgia Rheumatica,CPRD_PMR.csv,ICD_PMR.csv,
Portal hypertension,CPRD_portal_htn.csv,ICD_portal_htn.csv,
Post-term infant,CPRD_post_term.csv,ICD_post_term.csv,
Postcoital and contact bleeding,CPRD_PCB.csv,ICD_PCB.csv,
Posterior Uveitis,CPRD_post_uveitis.csv,ICD_post_uveitis.csv,
Postinfective and reactive arthropathies,CPRD_reactive.csv,ICD_reactive.csv,
Postmenopausal bleeding,CPRD_PMB.csv,ICD_PMB.csv,
"Postviral fatigue syndrome, neurasthenia and fibromyalgia",CPRD_chronic_fatigue.csv,ICD_chronic_fatigue.csv,
Prematurity,CPRD_prematurity.csv,ICD_prematurity.csv,
Primary Malignancy_Bladder,CPRD_pri_bladder.csv,ICD_pri_bladder.csv,
Primary Malignancy_Bone and articular cartilage,CPRD_pri_bone.csv,ICD_pri_bone.csv,
"Primary Malignancy_Brain, Other CNS and Intracranial",CPRD_pri_brain.csv,ICD_pri_brain.csv,
Primary Malignancy_Breast,CPRD_pri_breast.csv,ICD_pri_breast.csv,
Primary Malignancy_Cervical,CPRD_pri_cervical.csv,ICD_pri_cervical.csv,
Primary Malignancy_Kidney and Ureter,CPRD_pri_kidney.csv,ICD_pri_kidney.csv,
Primary Malignancy_Liver,CPRD_pri_liver.csv,ICD_pri_liver.csv,
Primary Malignancy_Lung and trachea,CPRD_pri_lung.csv,ICD_pri_lung.csv,
Primary Malignancy_Malignant Melanoma,CPRD_pri_melanoma.csv,ICD_pri_melanoma.csv,
Primary Malignancy_Mesothelioma,CPRD_pri_mesothelioma.csv,ICD_pri_mesothelioma.csv,
Primary Malignancy_Multiple independent sites,CPRD_pri_multindep.csv,ICD_pri_multindep.csv,
Primary Malignancy_Oesophageal,CPRD_pri_oesoph.csv,ICD_pri_oesoph.csv,
Primary Malignancy_Oro-pharyngeal,CPRD_pri_oroph.csv,ICD_pri_oroph.csv,
Primary Malignancy_Other Organs,CPRD_pri_other.csv,ICD_pri_other.csv,
Primary Malignancy_Other Skin and subcutaneous tissue,CPRD_pri_skin.csv,ICD_pri_skin.csv,
Primary Malignancy_Ovarian,CPRD_pri_ovarian.csv,ICD_pri_ovarian.csv,
Primary Malignancy_Pancreatic,CPRD_pri_pancr.csv,ICD_pri_pancr.csv,
Primary Malignancy_Prostate,CPRD_pri_prost.csv,ICD_pri_prost.csv,
Primary Malignancy_Stomach,CPRD_pri_stomach.csv,ICD_pri_stomach.csv,
Primary Malignancy_Testicular,CPRD_pri_testis.csv,ICD_pri_testis.csv,
Primary Malignancy_Thyroid,CPRD_pri_thyroid.csv,ICD_pri_thyroid.csv,
Primary Malignancy_Uterine,CPRD_pri_uterine.csv,ICD_pri_uterine.csv,
Primary Malignancy_biliary tract,CPRD_pri_biliary.csv,ICD_pri_biliary.csv,
Primary Malignancy_colorectal and anus,CPRD_pri_bowel.csv,ICD_pri_bowel.csv,
Primary or Idiopathic Thrombocytopaenia,CPRD_pri_thrombocytopaenia.csv,ICD_pri_thrombocytopaenia.csv,
Primary pulmonary hypertension,CPRD_prim_pulm_htn.csv,ICD_prim_pulm_htn.csv,
Psoriasis,CPRD_psoriasis.csv,ICD_psoriasis.csv,
Psoriatic arthropathy,CPRD_PSA.csv,ICD_PSA.csv,
Ptosis of eyelid,CPRD_ptosis.csv,ICD_ptosis.csv,OPCS_ptosis.csv
Pulmonary collapse (excl pneumothorax),CPRD_pulm_collapse.csv,ICD_pulm_collapse.csv,
Pulmonary embolism,CPRD_PE.csv,ICD_PE.csv,
Raynaud's syndrome,CPRD_raynauds.csv,ICD_raynauds.csv,
Respiratory distress of newborn,CPRD_RDN.csv,ICD_RDN.csv,
Respiratory failure,CPRD_resp_failure.csv,ICD_resp_failure.csv,
Retinal detachments and breaks,CPRD_retinal_detach.csv,ICD_retinal_detach.csv,OPCS_retinal_detach.csv
Retinal vascular occlusions,CPRD_retinal_vasc_occl.csv,ICD_retinal_vasc_occl.csv,
Rheumatic fever,CPRD_rh_fever.csv,ICD_rh_fever.csv,
Rheumatic valve dz,CPRD_Rh_valve.csv,ICD_Rh_valve.csv,
Rheumatoid Arthritis,CPRD_RhA.csv,ICD_RhA.csv,
Right bundle branch block,CPRD_RBBB.csv,ICD_RBBB.csv,
Rosacea,CPRD_rosacea.csv,ICD_rosacea.csv,
Sarcoidosis,CPRD_sarcoid.csv,ICD_sarcoid.csv,
"Schizophrenia, schizotypal and delusional disorders",CPRD_schizo.csv,ICD_schizo.csv,
Scleritis and episcleritis,CPRD_scleritis.csv,ICD_scleritis.csv,
Scoliosis,CPRD_scoliosis.csv,ICD_scoliosis.csv,
Seborrheic dermatitis,CPRD_seb_derm.csv,ICD_seb_derm.csv,
Secondary Malignancy_Adrenal gland,CPRD_sec_adrenal.csv,ICD_sec_adrenal.csv,
Secondary Malignancy_Bone,CPRD_sec_bone.csv,ICD_sec_bone.csv,
Secondary Malignancy_Bowel,CPRD_sec_bowel.csv,ICD_sec_bowel.csv,
"Secondary Malignancy_Brain, Other CNS and Intracranial",CPRD_sec_brain.csv,ICD_sec_brain.csv,
Secondary Malignancy_Lung,CPRD_sec_lung.csv,ICD_sec_lung.csv,
Secondary Malignancy_Lymph Nodes,CPRD_sec_LN.csv,ICD_sec_LN.csv,
Secondary Malignancy_Other organs,CPRD_sec_other.csv,ICD_sec_other.csv,
Secondary Malignancy_Pleura,CPRD_sec_pleura.csv,ICD_sec_pleura.csv,
Secondary Malignancy_retroperitoneum and peritoneum,CPRD_sec_peritoneum.csv,ICD_sec_peritoneum.csv,
Secondary malignancy_Liver and intrahepatic bile duct,CPRD_sec_liver.csv,ICD_sec_liver.csv,
Secondary or other Thrombocytopaenia,CPRD_sec_oth_thrombocytopaenia.csv,ICD_sec_oth_thrombocytopaenia.csv,
Secondary polycythaemia,CPRD_2ry_polycythaemia.csv,ICD_2ry_polycythaemia.csv,
Secondary pulmonary hypertension,CPRD_sec_pulm_htn.csv,ICD_sec_pulm_htn.csv,
Septicaemia,,ICD_sepsis.csv,
Sick sinus syndrome,CPRD_sick_sinus.csv,ICD_sick_sinus.csv,
Sickle-cell anaemia,CPRD_sickle_cell.csv,ICD_sickle_cell.csv,
Sickle-cell trait,CPRD_sickle_trait.csv,ICD_sickle_trait.csv,
Sjogren's disease,CPRD_sjogren.csv,ICD_sjogren.csv,
Sleep apnoea,CPRD_sleep_apnoea.csv,ICD_sleep_apnoea.csv,
Slow fetal growth or low birth weight,CPRD_LBW.csv,ICD_LBW.csv,
Spina bifida,CPRD_spina_bifida.csv,ICD_spina_bifida.csv,OPCS_spina_bifida.csv
Spinal stenosis,CPRD_spinal_stenosis.csv,ICD_spinal_stenosis.csv,
Splenomegaly,CPRD_splenomegaly.csv,ICD_splenomegaly.csv,
Spondylolisthesis,CPRD_spondylolisthesis.csv,ICD_spondylolisthesis.csv,
Spondylosis,CPRD_spondylosis.csv,ICD_spondylosis.csv,
Stable angina,CPRD_stable_angina.csv,ICD_stable_angina.csv,
Stroke NOS,CPRD_Stroke_NOS.csv,ICD_Stroke_NOS.csv,
Subarachnoid haemorrhage,CPRD_Subarach.csv,ICD_Subarach.csv,
Subdural haematoma - nontraumatic,CPRD_subdural_haem.csv,ICD_subdural_haem.csv,
Supraventricular tachycardia,CPRD_SVT.csv,ICD_SVT.csv,
Syndrome of inappropriate secretion of antidiuretic hormone,,ICD_SIADH.csv,
Systemic sclerosis,CPRD_sys_sclerosis.csv,ICD_sys_sclerosis.csv,
Thalassaemia,CPRD_thala.csv,ICD_thala.csv,
Thalassaemia trait,CPRD_thal_trait.csv,ICD_thal_trait.csv,
Thrombophilia,CPRD_thrombophilia.csv,ICD_thrombophilia.csv,
Tinnitus,CPRD_tinnitus.csv,ICD_tinnitus.csv,
Transient ischaemic attack,CPRD_TIA.csv,ICD_TIA.csv,
Trifascicular block,CPRD_trifasc_block.csv,ICD_trifasc_block.csv,
Trigeminal neuralgia,CPRD_trigem_neur.csv,ICD_trigem_neur.csv,
Tuberculosis,CPRD_TB.csv,ICD_TB.csv,
Tubulo-interstitial nephritis,CPRD_TIN.csv,ICD_TIN.csv,
Ulcerative colitis,CPRD_ulc_colitis.csv,ICD_ulc_colitis.csv,
Undescended testicle,CPRD_undescended_testis.csv,ICD_undescended_testis.csv,
Unstable Angina,CPRD_unstable_angina.csv,ICD_unstable_angina.csv,
Urinary Incontinence,CPRD_urine_incont.csv,ICD_urine_incont.csv,
Urinary Tract Infections,,ICD_uti.csv,
Urolithiasis,CPRD_urolithiasis.csv,ICD_urolithiasis.csv,OPCS_urolithiasis.csv
Urticaria,CPRD_urticaria.csv,ICD_urticaria.csv,
Venous thromboembolic disease (Excl PE),CPRD_vte_ex_pe.csv,ICD_vte_ex_pe.csv,
Ventricular tachycardia,CPRD_VT.csv,ICD_VT.csv,
Viral diseases (excl chronic hepatitis/HIV),,ICD_viral.csv,
Visual impairment and blindness,CPRD_blind.csv,ICD_blind.csv,
Vitamin B12 deficiency anaemia,CPRD_b12_def.csv,ICD_b12_def.csv,
Vitiligo,CPRD_vitiligo.csv,ICD_vitiligo.csv,
Volvulus,CPRD_volvulus.csv,ICD_volvulus.csv,OPCS_volvulus.csv
Chronic Kidney Disease,,,
Low HDL-C,,,
Raised LDL-C,,,
Raised Total Cholesterol,,,
Raised Triglycerides,,,"""

import io
import pandas as pd

dictionary = pd.read_csv(io.StringIO(dictionary))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Transform to match TRE layout  
# MAGIC   
# MAGIC 1. Map `.csv` to TRE tables:  
# MAGIC   1. Strip `.csv`  
# MAGIC   2. Add `caliber_` prefix  
# MAGIC 2. Account for conversion of Read/Med codes -> SNOMED within TRE:  
# MAGIC   1. Add snomedct column: copy of CPRD with suffix `_snomedct`  
# MAGIC 2. Phenotype regexps:  
# MAGIC   1. Strip from phenotypes, as early as possible: 
# MAGIC     1. Apostrophes/single quotes from phenotype names, e.g. `Crohn's disease` which can cause errors when parameterised into SQL queries
# MAGIC     1. Brackets e.g. `Lupus erythematosus (local and systemic)`  
# MAGIC     2. Commas e.g. `Postviral fatigue syndrome, neurasthenia and fibromyalgia`  
# MAGIC     3. Ampersand e.g. `Enthesopathies & synovial disorders`  
# MAGIC     4. Forward slash e.g. `Dermatitis (atopc/contact/other/unspecified)` -> underscore
# MAGIC   2. Consistency:  
# MAGIC     1. lower case  
# MAGIC     2. whitespace -> underscore  
# MAGIC 3. Specify database params
# MAGIC   1. Add `database`
# MAGIC   2. Create full `path`  
# MAGIC 4. `spark`-ify  
# MAGIC   
# MAGIC   
# MAGIC ### Target:  
# MAGIC 
# MAGIC | Phenotype    | Terminology | Table |  Database |  Path | 
# MAGIC | ----------- | ----------- |
# MAGIC | `Abdominal aortic aneurysm`	   |  `SNOMED` | `caliber_cprd_aaa_snomedct` |  `bhf_cvd_covid_uk_byod` |  `bhf_cvd_covid_uk_byod.caliber_cprd_aaa_snomedct` |

# COMMAND ----------

# 1. Map .csv to TRE tables
dictionary = dictionary.stack().str.replace('\\.csv', "").unstack()
dictionary['SNOMEDCT'] = dictionary['CPRD'].apply(lambda x: "{}{}".format(x, '_snomedct'))
dictionary = dictionary.melt(id_vars='phenotype', value_vars=['CPRD', 'ICD', 'OPCS', 'SNOMEDCT'], var_name='terminology', value_name='table')
dictionary = dictionary.replace('nan_snomedct', None)
dictionary = dictionary.dropna()
dictionary['table'] = dictionary['table'].str.lower()
dictionary['table'] = dictionary['table'].apply(lambda x: "{}{}".format('caliber_', x))
# 3. Phenotype REGEX
dictionary['phenotype'] = dictionary['phenotype'].str.replace("\\'", "")
dictionary['phenotype'] = dictionary['phenotype'].str.replace("\\(", "")
dictionary['phenotype'] = dictionary['phenotype'].str.replace("\\)", "")
dictionary['phenotype'] = dictionary['phenotype'].str.replace("\\,", "")
dictionary['phenotype'] = dictionary['phenotype'].str.replace("\\&", "")
dictionary['phenotype'] = dictionary['phenotype'].str.replace("\\/", "_")
dictionary['phenotype'] = dictionary['phenotype'].str.lower()
dictionary['phenotype'] = dictionary['phenotype'].str.replace(" ", "_")
# Specify database params
dictionary['database'] = 'bhf_cvd_covid_uk_byod'
dictionary['path'] = dictionary['database'] + "." + dictionary['table']

# spark-ify
dictionary = spark.createDataFrame(dictionary)
display(dictionary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Write table

# COMMAND ----------

dictionary.createOrReplaceGlobalTempView("ccu013_caliber_dictionary")
drop_table("ccu013_caliber_dictionary") 
create_table("ccu013_caliber_dictionary") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Optimise `delta table`

# COMMAND ----------

spark.sql("""OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_caliber_dictionary""")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Build master codelist
# MAGIC 
# MAGIC Target:  
# MAGIC 
# MAGIC | Phenotype    | Terminology | Code |
# MAGIC | ----------- | ----------- |
# MAGIC | `abdominal_aortic_aneurysm`	   |  `SNOMED` | `xxxx` |
# MAGIC |   |   | `xxxx` |
# MAGIC |   |  `ICD` | `xxxx` |
# MAGIC |   |  `OPCS` | `xxxx` |
# MAGIC 
# MAGIC 
# MAGIC **Check - are Medcodes & Readcodes used within TRE?**  
# MAGIC **ANSWER = not at present**  
# MAGIC Maybe for historical records?  
# MAGIC If so easiest (but definitely not most efficient) way is probably to duplicate the CPRD entires, set half to Readcode, half to Medcode  
# MAGIC Then just read the relevant column for each half  
# MAGIC ? find a way to efficiently read the 2 cols into 1 col, something like reduce maybe?  

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Remove CPRD - no readcodes & medcodes in the TRE

# COMMAND ----------

# Temporarily remove CPRD as there don't seem to be readcodes & medcodes in the TRE

dictionary = spark.sql("""
SELECT
  *
FROM
  dars_nic_391419_j3w9t_collab.ccu013_caliber_dictionary
WHERE
  terminology != 'CPRD'
""").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Define master codelist constructor functions

# COMMAND ----------

phenos = dictionary['phenotype'].tolist()
terms = dictionary['terminology'].tolist()
paths = dictionary['path'].tolist()

# COMMAND ----------

import re

# Pass 'path' rather than 'table' as this way we can use same var to construct query
# Print statements commented out in a bid to improve speed, can uncomment to debug

def getCodeforTable(path):
  if re.search('icd', path):
    code = 'ICD10code'
    #print("Table contains ICD-10 codes")
  elif re.search('snomedct', path):
    code = 'conceptId'
    #print("Table contains SNOMEDCT codes")
  elif re.search('cprd', path) and not re.search('snomedct', path):
    # 2 codes, check how this works?
    # Doesn't seem to be present in TRE therefore can we just remove this?! 
    code = 'Readcode, Medcode'
    #print("Table contains BOTH Readcodes & Medcodes")
  elif re.search('opcs', path):
    code = 'OPCS4code'
    #print("Table contains OPCS4 codes")
  else:
    raise ValueError('Table at:', path, 'name does not contain a terminology string match')
  return(code)

# Pass 'path' rather than 'table' as this way we can use same var to construct query

def getQuery(path, pheno, term):
  code = getCodeforTable(path)
  #print("Phenotype:", pheno, "Terminology:", term, "Code cols:", code)
  # Now need to build query with these
  query = """SELECT '{}' as phenotype, '{}' as terminology, {} as code FROM {}""".format(pheno, term, code, path)
  #print(query)
  return(query)
# Return: Phenotype	Terminology	Code


def runQuery(path, pheno, term):
  return(spark.sql(getQuery(path, pheno, term)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Build master codelist
# MAGIC **Warning the below query takes a LONG time ~ 2 HOURS**

# COMMAND ----------

# Initialise spark dataframe
master_codelist = spark.createDataFrame(
  [
    ('test_pheno', 'test_term', 1234)
  ],
  ['phenotype','terminology','code']
)

count = 1
total = 635

for pheno, term, path in zip(phenos, terms, paths):
  print(count, "/", total, "Importing: ", pheno, "From: ", path)
  new_query = runQuery(path, pheno, term)
  master_codelist = master_codelist.union(new_query)
  print(count, "/", total, "Completed: ", pheno, "From: ", path)
  count += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Codelist tidying
# MAGIC 1. Remove decimal point from `ICD10` codes as recorded without this in HES

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

master_codelist = master_codelist.withColumn('code', regexp_replace(col("code"), "\\.", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Write `ccu013_caliber_master_codelist`

# COMMAND ----------

master_codelist.createOrReplaceGlobalTempView('ccu013_caliber_master_codelist')
drop_table("ccu013_caliber_master_codelist")
create_table("ccu013_caliber_master_codelist") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Optimise delta table

# COMMAND ----------

spark.sql("""OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_caliber_master_codelist""")

# COMMAND ----------

master_codelist.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Check counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(code), COUNT(distinct phenotype)
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_caliber_master_codelist

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   distinct phenotype
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_caliber_master_codelist

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   phenotype,
# MAGIC   COUNT(distinct code) as n_codes,
# MAGIC   COUNT(distinct terminology) as n_terminologies
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t_collab.ccu013_caliber_master_codelist
# MAGIC GROUP BY
# MAGIC   phenotype
