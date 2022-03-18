## R script to create demographic table for the three different COVID-19 death events identified
#    - Table output used in the COVID-Phenotype severity paper
# Authors: Johan Hilge Thygesen
# Last updated: 07.09.21

library("dplyr")

demo <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort")
traject <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort")
demo$age <- as.numeric(demo$age)

cohort <- dbGetQuery(con, "SELECT * FROM (SELECT 
a.person_id_deid, 
CASE WHEN wave1 == 1 THEN 1 WHEN wave2 == 1 THEN 2 else NULL END AS wave, 
death, 02_Covid_admission, 03_ECMO_treatment, 03_ICU_admission, 03_IMV_treatment, 03_NIV_treatment 
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort as a
LEFT JOIN (SELECT person_id_deid, 1 as wave1 FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival_wave1) as b
ON a.person_id_deid = b.person_id_deid
LEFT JOIN (SELECT person_id_deid, 1 as wave2 FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival_wave2) as c
ON a.person_id_deid = c.person_id_deid)")


cohort <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort")
cohort[,"followup"] <- ifelse(!is.na(cohort[,"date_death"]), 
                              as.numeric(difftime(cohort[,"date_death"], cohort[,"date_first"], units = "days")),
                              as.numeric(difftime(as.Date("31-03-2021", format = "%d-%m-%Y"), cohort[,"date_first"], units = "days")))
wave_1_start = "2020-03-20"
wave_1_end = "2020-05-29"
wave_2_start = "2020-09-30"
wave_2_end = "2021-02-12"

cohort %<>% 
  mutate(wave = if_else(date_first >= wave_1_start & 
                          date_first <= wave_1_end, 
                        1, 1.5) # name inter-wave period as 1.5
  ) %>%
  mutate(wave = if_else(date_first >= wave_2_start & 
                          date_first <= wave_2_end, 
                        2, wave)
  ) %>% 
  select(-date_first)




# --- Create demographics table ---------------------
length(unique(demo$person_id_deid))
table(traject$covid_phenotype)

covid_events <- c("01_Covid_positive_test", "01_GP_covid_diagnosis", "02_Covid_admission", "03_ICU_admission", "03_NIV_treatment", "03_IMV_treatment",
                  "03_ECMO_treatment", "04_Fatal_with_covid_diagnosis", "04_Fatal_without_covid_diagnosis", "04_Covid_inpatient_death")

groups <- list(death_with_diagnosis = unique(traject[which(traject$covid_phenotype == "04_Fatal_with_covid_diagnosis"), "person_id_deid"]), 
               death_without_diagnosis = unique(traject[which(traject$covid_phenotype == "04_Fatal_without_covid_diagnosis"), "person_id_deid"]),
               inpatient_deaths = unique(traject[which(traject$covid_phenotype == "04_Covid_inpatient_death"), "person_id_deid"]),
               death_no_hospital = unique(cohort[which(cohort[,"death"]==1 & cohort[,"02_Covid_admission"] == 0 & 
                                                         cohort[,"03_ECMO_treatment"] == 0 &
                                                         cohort[,"03_ICU_admission"] == 0 &
                                                         cohort[,"03_IMV_treatment"] == 0 &
                                                         cohort[,"03_NIV_treatment"] == 0),"person_id_deid"]),
               death_no_hospital_wave1 = unique(cohort[which(cohort[,"death"]==1 & cohort[,"02_Covid_admission"] == 0 & 
                                                               cohort[,"03_ECMO_treatment"] == 0 &
                                                               cohort[,"03_ICU_admission"] == 0 &
                                                               cohort[,"03_IMV_treatment"] == 0 &
                                                               cohort[,"03_NIV_treatment"] == 0 &
                                                               cohort[,"wave"] == 1),"person_id_deid"]),
               death_no_hospital_wave2 = unique(cohort[which(cohort[,"death"]==1 & cohort[,"02_Covid_admission"] == 0 & 
                                                               cohort[,"03_ECMO_treatment"] == 0 &
                                                               cohort[,"03_ICU_admission"] == 0 &
                                                               cohort[,"03_IMV_treatment"] == 0 &
                                                               cohort[,"03_NIV_treatment"] == 0 &
                                                               cohort[,"wave"] == 2),"person_id_deid"]),
               all = unique(demo$person_id_deid))

out <- data.frame()
for(i in 1:length(groups)){
  uniid <- length(unique(as.character(unlist(groups[i]))))
  if(grepl("wave1", names(groups[i]))){
    n <- paste0(uniid, " (", round(uniid / length(unique(cohort[which(cohort[,"wave"]==1),"person_id_deid"])) * 100, 1), ")")
  }else if(grepl("wave2", names(groups[i]))) {
    n <- paste0(uniid, " (", round(uniid / length(unique(cohort[which(cohort[,"wave"]==2),"person_id_deid"])) * 100, 1), ")")
  }else{
    n <- paste0(uniid, " (", round(uniid / length(unique(traject$person_id_deid)) * 100, 1), ")")
  }
  out['n', names(groups)[i]] <- n
  out["gender",names(groups)[i]] <- ""
  gender <- unique(demo[which(demo[,"person_id_deid"] %in% as.character(unlist(groups[i]))),c('person_id_deid','sex')])
  gender$sex <- as.numeric(gender$sex)
  out["female",names(groups)[i]] <- paste0(sum(gender$sex==2, na.rm = T), " (", round(sum(gender$sex==2, na.rm = T) / sum(!is.na(gender$sex)) * 100,1), ")")
  out["unknown_gender",names(groups)[i]] <- paste0(sum(is.na(gender$sex)), " (", round(sum(is.na(gender$sex)) / uniid * 100,1), ")")
  out["age",names(groups)[i]] <- ""
  age <- unique(demo[which(demo[,"person_id_deid"] %in% as.character(unlist(groups[i]))),c('person_id_deid','age')])
  age[which(age$age > 130 | age$age < 0), "age"] <- NA
  out["age_18",names(groups)[i]] <- paste0(sum(age$age<18, na.rm = T), " (", round(sum(age$age<18, na.rm = T)/ sum(!is.na(age$age)) * 100,1), ")")
  out["age_18_29",names(groups)[i]] <- paste0(sum(age$age>=18 & age$age <=29, na.rm = T), " (", round(sum(age$age>=18 & age$age <=29, na.rm = T)/ sum(!is.na(age$age)) * 100,1), ")")
  out["age_30_49",names(groups)[i]] <- paste0(sum(age$age>=30 & age$age <=49, na.rm = T), " (", round(sum(age$age>=30 & age$age <=49, na.rm = T)/ sum(!is.na(age$age)) * 100,1), ")")
  out["age_50_69",names(groups)[i]] <- paste0(sum(age$age>=50 & age$age <=69, na.rm = T), " (", round(sum(age$age>=50 & age$age <=69, na.rm = T)/ sum(!is.na(age$age)) * 100,1), ")")
  out["age_70",names(groups)[i]] <- paste0(sum(age$age>=70, na.rm = T), " (", round(sum(age$age>=70, na.rm = T)/ sum(!is.na(age$age)) * 100,1), ")")
  out["unknown_age",names(groups)[i]] <- paste0(sum(is.na(age$age)), " (", round(sum(is.na(age$age)) / uniid * 100,1), ")")
  ethnicity <- unique(demo[which(demo[,"person_id_deid"] %in% as.character(unlist(groups[i]))),c('person_id_deid','ethnic_group')])
  ethnicity[which(ethnicity$ethnic_group=="Unknown"), "ethnic_group"] <- NA
  out["ethnicity",names(groups)[i] ] <- ""
  out["white",names(groups)[i]] <- paste0(sum(ethnicity$ethnic_group == "White", na.rm = T), " (",
                                          round(sum(ethnicity$ethnic_group == "White", na.rm = T) / uniid * 100,1) ,")")
  out["asian_or_asian_british",names(groups)[i]] <- paste0(sum(ethnicity$ethnic_group == "Asian or Asian British", na.rm = T), " (",
                                                           round(sum(ethnicity$ethnic_group == "Asian or Asian British", na.rm = T) / uniid * 100,1) ,")")
  out["black_or_black_british",names(groups)[i]] <- paste0(sum(ethnicity$ethnic_group == "Black or Black British", na.rm = T), " (",
                                                           round(sum(ethnicity$ethnic_group == "Black or Black British", na.rm = T) / uniid * 100,1) ,")")
  out["chinese",names(groups)[i]] <- paste0(sum(ethnicity$ethnic_group == "Chinese", na.rm = T), " (",
                                            round(sum(ethnicity$ethnic_group == "Chinese", na.rm = T) / uniid * 100,1) ,")")
  out["mixed_and_others",names(groups)[i]] <- paste0(sum(ethnicity$ethnic_group %in% c("Mixed", "Other"), na.rm = T), " (",
                                                     round(sum(ethnicity$ethnic_group %in% c("Mixed", "Other"), na.rm = T) / uniid * 100,1) ,")")
  out["unknown_ethnicity",names(groups)[i]] <- paste0(sum(is.na(ethnicity$ethnic_group)), " (",
                                                      round(sum(is.na(ethnicity$ethnic_group)) / uniid * 100,1) ,")")
  out["imd_fifths", names(groups)[i]] <- ""
  imd <- unique(demo[which(demo[,"person_id_deid"] %in% as.character(unlist(groups[i]))),c('person_id_deid','IMD_quintile')])
  imd[which(imd[,"IMD_quintile"]=="Unknown"), "IMD_quintile"] <- NA
  out["imd_1", names(groups)[i]] <- paste0(sum(imd$IMD_quintile=="1", na.rm = T), " (",
                                           round(sum(imd$IMD_quintile=="1",na.rm = T) / length(imd$IMD_quintile) * 100,1) ,")")
  out["imd_5", names(groups)[i]] <- paste0(sum(imd$IMD_quintile=="5", na.rm = T), " (",
                                           round(sum(imd$IMD_quintile=="5", na.rm = T) / length(imd$IMD_quintile) * 100,1) ,")")
  out["imd_unknown", names(groups)[i]] <- paste0(sum(is.na(imd$IMD_quintile)), " (",
                                                 round(sum(is.na(imd$IMD_quintile)) / length(imd$IMD_quintile) * 100,1) ,")")
  # Covid events
  events <- unique(traject[which(traject[,"person_id_deid"] %in% as.character(unlist(groups[i]))),c('person_id_deid','covid_phenotype')])
  out["Covid_events",names(groups)[i]] <- ""
  for(x in 1:length(covid_events)){
    out[covid_events[x],names(groups)[i]] <- paste0(sum(events$covid_phenotype == covid_events[x]), " (",
                                                    round(sum(events$covid_phenotype == covid_events[x]) / uniid * 100,1) ,")")
  }
}

out
write.table(out, "~/dars_nic_391419_j3w9t_collab/CCU013/output/tables/CCU013_supp_tab4_demographic_table_of_deaths.txt", sep = "\t", quote = F)
