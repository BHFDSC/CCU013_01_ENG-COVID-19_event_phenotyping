library(data.table)
library(dplyr)
library(survival)
library(survminer)

folder_path <- "~/dars_nic_391419_j3w9t_collab/CCU013/output/km_plots"
basefilename <- "CCU013_km_"

source("~/dars_nic_391419_j3w9t_collab/CCU013/04_analysis_R-GITHUB-v3/matching_function.R")

# Note this is exact matching, samples that cant be matched exactly are not included in the further analysis.

# Load sample for matching
## -------------------------------------------
d <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_feb_2021_vax_status_matching")

nrow(d) # 50357509 @230122 | old:49,929,434

case <- d[which(d$vaccine_status == "Vaccinated"), ]
ctrl <- d[which(d$vaccine_status == "Unvaccinated"), ]
case[,"vaccinated"] <- 1
ctrl[,"vaccinated"] <- 0
m <- rbind(case,ctrl)
m <- data.table(m)
m <- m[-which(m$SEX %in% c(0,9)), ]
nrow(m) # 49928374
table(m$vaccinated) # Unvax = 49,932,299 Vax = 425,148 @230122 |OLD: Unvax = 49,506,564, Vax = 421,810

m$mvar <- do.call('paste0', m[,c("SEX","ethnicity","age_grp")])
#m$mvar <- do.call('paste0', m[,c("age_grp")])
matched_m <- smatch(m, treat = "vaccinated", mvar = 'mvar', ratio = 1, seed = 42)
matched_summary <- dcast(matched_m, SEX + ethnicity + age_grp ~vaccinated, value.var = 'id', fun.aggregate = length)
print(paste0(max(matched_m$id), " out of ", nrow(case), " samples was matached exactly!"))

## For dosage 2: @230222 - "425147 out of 425148 samples was matached exactly!" | OLD: "421809 out of 421811 samples was matached exactly!"

# write out matching results
matched_m <- matched_m[order(matched_m$id), ]
write.table(matched_m[,c("person_id_deid", "id", "vaccinated")], "~/dars_nic_391419_j3w9t_collab/CCU013/output/matching/ccu013_vaccine_matchings.txt", row.names = F, sep = "\t", quote = F)
write.table(matched_summary, "~/dars_nic_391419_j3w9t_collab/CCU013/output/matching/ccu013_vaccine_matchings_summary.txt", row.names = F, sep = "\t", quote = F)

unmatched <- m[which(!m$person_id_deid %in% matched_m$person_id_deid), ]
unmatched[which(unmatched$vaccinated == 1), ]
head(unmatched)
sort(table(unmatched[which(unmatched$vaccinated == 1),".mvar"]))
sum(table(unmatched[which(unmatched$vaccinated == 1),".mvar"]))


# matched_m <- read.table("~/dars_nic_391419_j3w9t_collab/CCU013/output/matching/ccu013_vaccine_matchings.txt", header = T, sep = "\t")
head(matched_m)
length(unique(matched_m$id))
unmatched <- m[which(!m$person_id_deid %in% matched_m$person_id_deid), ]
table(unmatched[,".mvar"])


### Add in COVID data.
#-----------------------------------------------------------------------------------------------------------------------
## NOTE!! our date of interest is: end_date
start_date <- as.Date("2021-02-01")
end_date <- as.Date("2021-11-30") # as.Date("2021-05-31")
traject <- dbGetQuery(con, paste0("SELECT a.person_id_deid, date, covid_phenotype, death_date 
                                  FROM (SELECT person_id_deid, date, covid_phenotype FROM
                                  dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort 
                                  WHERE date >= \"",  as.character(start_date), "\") AS a
                                  LEFT JOIN (SELECT person_id_deid, min(death_date) as death_date FROM 
                                  dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths group by person_id_deid) AS b
                                  ON a.person_id_deid = b.person_id_deid 
                                  "))

uni_events <- unique(traject$covid_phenotype)

head(traject)

traject[which(traject[,"covid_phenotype"] %in% c("01_Covid_positive_test")),"event"] <- "Positive test"
traject[which(traject[,"covid_phenotype"] %in% c("01_GP_covid_diagnosis")),"event"] <- "Primary care diagnosis"
traject[which(traject[,"covid_phenotype"] %in% c("02_Covid_admission")),"event"] <- "Hospitalisation"
traject[which(traject[,"covid_phenotype"] %in% c("03_ICU_admission", "03_IMV_treatment", "03_NIV_treatment", "03_ECMO_treatment")),"event"] <- "Critical care"
traject[which(traject[,"covid_phenotype"] %in% c("04_Fatal_without_covid_diagnosis", "04_Covid_inpatient_death", "04_Fatal_with_covid_diagnosis")),"event"] <- "COVID-19 mortality"

unievents <- unique(traject$event)

matched_m[which(matched_m$vaccinated == 1),"group"] = "Vaccinated" 
matched_m[which(matched_m$vaccinated == 0),"group"] = "Unvaccinated" 
matched_m <- as.data.frame(matched_m)
matched_m[,"group"] <- factor(matched_m[,"group"], levels = c("Unvaccinated",  "Vaccinated" ))
table(matched_m$group, useNA ="always")
head(matched_m)

for(i in 1:length(unievents)){
  ## Get earliest date for event
  event <- traject %>%
    group_by(person_id_deid) %>%
    filter(event == unievents[i])  %>%
    arrange(date) %>%
    slice(1L)
  
  event <- as.data.frame(event)
  
  ## Add earliest date to matched data
  km_data <- merge(matched_m[,c("person_id_deid", "group")], event[,c("person_id_deid", "date", "death_date")], by = "person_id_deid", all.x = T) 
  
  km_data[,"event"] <- ifelse(is.na(km_data$date), 0, 1)
  #head(km_data)
  ## Cacluate fu_time
  
  km_data[,"event_fu_time"] <- ifelse(is.na(km_data$date),                         # Does the person experience the event?
                                      ifelse(is.na(km_data$death_date),              # Does the person die?
                                             as.numeric(end_date - start_date),        # If person is not dead - full study period
                                             as.numeric(km_data$death_date - start_date)),     # If person did die - death_date - start_date
                                      as.numeric(km_data$date - start_date))         # Person experienced the event so fu_time is up until event
  
  my_ymax <- ifelse(unievents[i] == "Positive test", 0.007, 
                    ifelse(unievents[i] == "Critical care", 0.0005,
                           ifelse(unievents[i] == "COVID-19 mortality", 0.002, 0.005)))

  ## Survival plot
  survp <- survfit(Surv(event_fu_time, event) ~ group,
                   data=km_data) %>% 
    ggsurvplot(pval = FALSE, conf.int = 0.95,
               ylim=c(0, my_ymax), # Have to manually adjust this to scale plot
               xlim=c(0, 42), # Probably makes sense to cut off plot at 28 days, as that's the gauranteed min fu_time
               break.time.by = 7,
               #pval.coord = c(0,  my_ymax-(my_ymax*0.1)),
               title = paste0(unievents[i], " - Stratified by vaccination status"),
               legend.title = "",
               legend.labs = c("Unvaccinated", "Vaccinated"),
               risk.table = TRUE,
               cumevents = TRUE,
               font.y = 8,
               fontsize = 4,
               fun = "event") # + theme(axis.text =  element_text(size = 12))
  
  options(scipen=999)
  pdf(paste0(folder_path, "/", basefilename, "vaccination_status_dose2_", gsub(" ", "_", tolower(unievents[i])), ".pdf"), width = 9)
  print(survp, newpage = FALSE)
  dev.off()
}
