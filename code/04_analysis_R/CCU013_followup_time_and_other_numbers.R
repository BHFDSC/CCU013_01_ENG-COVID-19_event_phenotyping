library(dplyr)

cohort <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort")
head(cohort)
nrow(cohort)
cohort[,"followup"] <- ifelse(!is.na(cohort[,"date_death"]), 
                              as.numeric(difftime(cohort[,"date_death"], cohort[,"date_first"], units = "days")),
                              as.numeric(difftime(as.Date("30-11-2021", format = "%d-%m-%Y"), cohort[,"date_first"], units = "days")))
mean(cohort$followup)
sd(cohort$followup)

##### Dying out-side of hospital
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


# People dying outside of hospital
n_death_outside_hospital <- nrow(cohort[which(cohort[,"death"]==1 & cohort[,"02_Covid_admission"] == 0 & 
                    cohort[,"03_ECMO_treatment"] == 0 &
                    cohort[,"03_ICU_admission"] == 0 &
                    cohort[,"03_IMV_treatment"] == 0 &
                    cohort[,"03_NIV_treatment"] == 0),])

n_death_outside_hospital_wave1 <- nrow(cohort[which(cohort[,"death"]==1 & cohort[,"02_Covid_admission"] == 0 & 
                                                cohort[,"03_ECMO_treatment"] == 0 &
                                                cohort[,"03_ICU_admission"] == 0 &
                                                cohort[,"03_IMV_treatment"] == 0 &
                                                cohort[,"03_NIV_treatment"] == 0 &
                                                cohort[,"wave"] == 1),])

n_death_outside_hospital_wave1 <- nrow(cohort[which(cohort[,"death"]==1 & cohort[,"02_Covid_admission"] == 0 & 
                                                      cohort[,"03_ECMO_treatment"] == 0 &
                                                      cohort[,"03_ICU_admission"] == 0 &
                                                      cohort[,"03_IMV_treatment"] == 0 &
                                                      cohort[,"03_NIV_treatment"] == 0 &
                                                      cohort[,"wave"] == 2),])

n_death_outside_hospital
n_death_outside_hospital/nrow(cohort) * 100

n_death_outside_hospital_wave1/nrow(cohort[which(cohort[,"wave"]==1),]) * 100
n_death_outside_hospital_wave1/nrow(cohort[which(cohort[,"wave"]==2),]) * 100

