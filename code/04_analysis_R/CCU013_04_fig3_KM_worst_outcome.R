library(dplyr)
library(survival)
library(survminer)
library(magrittr)

# Settings and data
#----------------------------------------------------------------------------------------------------------------------

# output vars
folder_path <- "~/dars_nic_391419_j3w9t_collab/CCU013/output/km_plots"
basefilename <- "CCU013_km_"

mycolors <- c("#74c476", "#006d2c", "#fe9929", "#ff6800", "#ff003e") # Positive test, GP diagnosis, Hospitalisation, ICU admission, Critical care outside ICU

# Main data
## Generated in Data bricks; Notebook CCU013_17_paper_survival_cohort
## Cohort data is used for stratifying on demographics; gender, age, ethnicity & IMD
cohort <- dbGetQuery(con, "SELECT person_id_deid, date_first, death, severity, death_fu_time FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival")
nrow(cohort) # 3469528

## Wave population are used to generate wave comparison plots
wave1 <- dbGetQuery(con, "SELECT person_id_deid, date_first, death, severity, death_fu_time FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival_wave1")
wave2 <- dbGetQuery(con, "SELECT person_id_deid, date_first, death, severity, death_fu_time FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_survival_wave2")
nrow(wave1) # 263839
nrow(wave2) # 2878573 
wave1[,"wave"] <- 1
wave2[,"wave"] <- 2
waves <- rbind(wave1, wave2)
waves <- as.data.frame(waves)

# Additional data
demo <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort")
nrow(demo)   # 3469528
vax <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_wave2_vax_status WHERE date is NOT NULL")
nrow(vax)    # 2957762

# Data prep 
#----------------------------------------------------------------------------------------------------------------------
# Merge waves with vax to get vacccination status 
waves <- merge(waves, vax[,c("person_id_deid", "vaccine_status")], by = "person_id_deid", all.x = T)

waves[which(waves$wave == 1), "Group"] <- "Wave 1"
waves[which(waves$wave == 2 & waves$vaccine_status == "Vaccinated"), "Group"] <- "Wave 2 - Vaccinated"
waves[which(waves$wave == 2 & waves$anyDose == 0), "Unvaccinated"] <- "Wave 2 - Unvaccinated"
waves[,"Group"] <- factor(waves[,"Group"], levels = c("Wave 1", "Wave 2 - Unvaccinated", "Wave 2 - Vaccinated"))
table(waves$Group)
head(waves)

# For some reason nulls filled -> 0 by pyspark reading as nulls? refresh time?
if(any(is.na(cohort$death))){
  cohort %<>% mutate(death = ifelse(is.na(death), 0 , 1))
}
table(cohort$death)

# Test that all deaths_only are dead
all(cohort[which(cohort$severity=="5_death_only"), "death"]==1)

# Remove deaths only
table(cohort$severity)
cohort <- cohort[-which(cohort$severity == "5_death_only"), ]
table(cohort$severity)

# Rename severity
table(cohort$severity)
cohort[which(cohort[, "severity"] == "0_positive"), "severity"] <- "Positive test"
cohort[which(cohort[, "severity"] == "1_gp"), "severity"] <- "Primary care diagnosis"
cohort[which(cohort[, "severity"] == "2_hospitalised"), "severity"] <- "Hospitalisation"
cohort[which(cohort[, "severity"] == "03_critical_care_outside_ICU"), "severity"] <- "Critical care outside ICU"
cohort[which(cohort[, "severity"] == "4_icu_admission"), "severity"] <- "ICU admission"
cohort$severity <- factor(cohort$severity, levels = c("Positive test", "Primary care diagnosis", "Hospitalisation", "ICU admission", "Critical care outside ICU"))
table(cohort$severity)

# Rename severity
waves[which(waves[, "severity"] == "0_positive"), "severity"] <- "Positive test"
waves[which(waves[, "severity"] == "1_gp"), "severity"] <- "Primary care diagnosis"
waves[which(waves[, "severity"] == "2_hospitalised"), "severity"] <- "Hospitalisation"
waves[which(waves[, "severity"] == "03_critical_care_outside_ICU"), "severity"] <- "Critical care outside ICU"
waves[which(waves[, "severity"] == "4_icu_admission"), "severity"] <- "ICU admission"
waves$severity <- factor(waves$severity, levels = c("Positive test", "Primary care diagnosis", "Hospitalisation", "ICU admission", "Critical care outside ICU"))
table(waves$severity)



# Plotting KM curves
#----------------------------------------------------------------------------------------------------------------------

### Waves 1 and wave 2
pdf(paste0(folder_path, "/", basefilename, "wave1-2_worst_outcome.pdf"), width = 14, height = 7)
survfit(Surv(death_fu_time, death) ~ severity,
        data=waves) %>% 
  ggsurvplot(facet.by = 'wave',  conf.int = 0.95,
             pval = FALSE, 
             ylim=c(0, 0.6), # Have to manually adjust this to scale plot
             xlim=c(0, 28), # Probably makes sense to cut off plot at 28 days, as that's the gauranteed min fu_time
             pval.coord = c(3, 0.35),
             break.time.by = 7,
             title = 'COVID-19 Mortality - Stratified by worst healthcare presentation',
             risk.table = TRUE,
             cumevents = TRUE,
             fun = "event"
             ) + scale_color_manual(values = mycolors) + scale_fill_manual(values = mycolors)
dev.off()

### Waves 1
f1 <- survfit(Surv(death_fu_time, death) ~ severity,
        data=waves[which(waves$wave == 1), ]) %>% 
  ggsurvplot(conf.int = 0.95,
             pval = FALSE, 
             ylim=c(0, 0.6), # Have to manually adjust this to scale plot
             xlim=c(0, 28), # Probably makes sense to cut off plot at 28 days, as that's the gauranteed min fu_time
             pval.coord = c(3, 0.35),
             break.time.by = 7,
             legend.title = "",
             legend.labs = c("Positive test", "Primary care diagnosis", "Hospitalisation", "ICU admission", "Critical care outside ICU"),
             font.y = 8,
             fontsize = 4,
             title = 'COVID-19 Mortality - Stratified by worst healthcare presentation',
             risk.table = TRUE,
             cumevents = TRUE,
             fun = "event",
             palette = mycolors
)
pdf(paste0(folder_path, "/", basefilename, "wave1_worst_outcome.pdf"), width = 8, height = 9)
print(f1, newpage = FALSE)
dev.off()

### Waves 2 unvax
f2 <- survfit(Surv(death_fu_time, death) ~ severity,
              data=waves[which(waves$wave == 2), ]) %>% 
  ggsurvplot(conf.int = 0.95,
             pval = FALSE, 
             ylim=c(0, 0.6), # Have to manually adjust this to scale plot
             xlim=c(0, 28), # Probably makes sense to cut off plot at 28 days, as that's the gauranteed min fu_time
             pval.coord = c(3, 0.35),
             break.time.by = 7,
             legend.title = "",
             legend.labs = c("Positive test", "Primary care diagnosis", "Hospitalisation", "ICU admission", "Critical care outside ICU"),
             font.y = 8,
             fontsize = 4,
             title = 'COVID-19 Mortality - Stratified by worst healthcare presentation',
             risk.table = TRUE,
             cumevents = TRUE,
             fun = "event",
             palette = mycolors
) 
pdf(paste0(folder_path, "/", basefilename, "wave2_worst_outcome.pdf"), width = 8, height = 9)
print(f2, newpage = FALSE)
dev.off()


### Define subgroups for supplement rbind and plot together with facet
subgroup <- cohort
subgroup[,"group"] <- "All"

male <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$sex ==1), "person_id_deid"]),]
male[,"group"] <- "Male"
subgroup <- rbind(male, subgroup)

female <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$sex ==2), "person_id_deid"]),]
female[,"group"] <- "Female"
subgroup <- rbind(female, subgroup)

age1 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$age<18), "person_id_deid"]),]
age1[,"group"] <- "Age under 18"
subgroup <- rbind(age1, subgroup)

age2 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$age>=18 & demo$age <=29), "person_id_deid"]),]
age2[,"group"] <- "Age 18 - 29"
subgroup <- rbind(age2, subgroup)

age3 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$age>=30 & demo$age <=49), "person_id_deid"]),]
age3[,"group"] <- "Age 30 - 49"
subgroup <- rbind(age3, subgroup)

age4 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$age>=50 & demo$age <=69), "person_id_deid"]),]
age4[,"group"] <- "Age 50 - 69"
subgroup <- rbind(age4, subgroup)

age5 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$age>=70), "person_id_deid"]),]
age5[,"group"] <- "Age 70 or above"
subgroup <- rbind(age5, subgroup)


unique(subgroup$group)
subgroup$group <- factor(subgroup$group, levels = c("All", "Male", "Female", "Age under 18", "Age 18 - 29", "Age 30 - 49", 
                                                    "Age 50 - 69", "Age 70 or above"))


pdf(paste0(folder_path, "/", basefilename, "gender_age.pdf"), width = 14, height = 14)
survfit(Surv(death_fu_time, death) ~ severity,
        data=subgroup) %>% 
  ggsurvplot(conf.int = 0.95,
             facet.by = 'group',
             pval = TRUE, 
             ylim=c(0, 0.6), # Have to manually adjust this to scale plot
             xlim=c(0, 28), # Probably makes sense to cut off plot at 28 days, as that's the gauranteed min fu_time
             pval.coord = c(3, 0.55),
             break.time.by = 7,
             title = 'COVID-19 Mortality - Stratified by worst healthcare presentation',
             risk.table = TRUE,
             fun = "event",
             palette = mycolors
  ) + theme(legend.title = element_blank(),
            legend.position = "bottom",
            legend.text = element_text(size = 12),
            strip.text = element_text(size = 14),
  )
dev.off()


## Ethnicity and LOS ###################
ethnicity1 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "White"), "person_id_deid"]),]
ethnicity1[,"group"] <- "Ethnicity white"
subgroup2 <- ethnicity1

ethnicity2 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Asian or Asian British"), "person_id_deid"]),]
ethnicity2[,"group"] <- "Ethnicity Asian or Asian British"
subgroup2 <- rbind(subgroup2, ethnicity2)

ethnicity3 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Black or Black British"), "person_id_deid"]),]
ethnicity3[,"group"] <- "Ethnicity Black or Black British"
subgroup2 <- rbind(subgroup2, ethnicity3)

ethnicity4 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Chinese"), "person_id_deid"]),]
ethnicity4[,"group"] <- "Ethnicity Chinese"
subgroup2 <- rbind(subgroup2, ethnicity4)

ethnicity5 <-cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Mixed"), "person_id_deid"]),]
ethnicity5[,"group"] <- "Ethnicity mixed"
subgroup2 <- rbind(subgroup2, ethnicity5)

ethnicity6 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Other"), "person_id_deid"]),]
ethnicity6[,"group"] <- "Ethnicity other"
subgroup2 <- rbind(subgroup2, ethnicity6)

imd1 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$IMD_quintile == 1), "person_id_deid"]),]
imd1[,"group"] <- "IMD quantile 1"
subgroup2 <- rbind(subgroup2, imd1)

imd2 <- cohort[which(cohort[,"person_id_deid"] %in% demo[which(demo$IMD_quintile == 5), "person_id_deid"]),]
imd2[,"group"] <- "IMD quantile 5"
subgroup2 <- rbind(subgroup2, imd2)

unique(subgroup2$group)
subgroup2$group <- factor(subgroup2$group, levels = c("Ethnicity white", "Ethnicity Asian or Asian British", "Ethnicity Black or Black British", "Ethnicity Chinese", 
                                                    "Ethnicity mixed", "Ethnicity other", 
                                                    "IMD quantile 1", "IMD quantile 5"))

head(sub)

pdf(paste0(folder_path, "/", basefilename, "ethnicity_imd.pdf"), width = 14, height = 14)
survfit(Surv(death_fu_time, death) ~ severity,
        data=subgroup2) %>% 
  ggsurvplot(conf.int = 0.95,
    facet.by = 'group',
             pval = TRUE, 
             ylim=c(0, 0.4), # Have to manually adjust this to scale plot
             xlim=c(0, 28), # Probably makes sense to cut off plot at 28 days, as that's the gauranteed min fu_time
             pval.coord = c(3, 0.35),
             break.time.by = 7,
             title = 'COVID-19 Mortality - Stratified by worst healthcare presentation',
             risk.table = TRUE,
             fun = "event",
    palette = mycolors
  ) + theme(legend.title = element_blank(),
            legend.position = "bottom",
            legend.text = element_text(size = 12),
            strip.text = element_text(size = 14),
  )
dev.off()

