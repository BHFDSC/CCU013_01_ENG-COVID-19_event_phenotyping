## R script to create patient trajectory network plots
#    - Plots used for figures in the COVID-Phenotype severity paper
# Authors: Johan Hilge Thygesen
# Last updated: 15.09.21

library(igraph)
library(scales)
library(dplyr)
source("CCU013_colorscheme.R")
source("trejectory_plot_function.R")

folder_path <- "~/dars_nic_391419_j3w9t_collab/CCU013/output/trajectories/"
basefilename <- "CCU013_trajectories_ICU"

N_total_pop <- 57032174

# X and Y coordinates for phenotype node location on plot - note values are relative to each other, i.e. coordinate are not used as absolute values!
graph_layout <- data.frame(phenotype = c("Unaffected","Positive test", "Primary care diagnosis", "Hospitalisation", "ICU admission", "Death"),
                           x = c(1, 2, 2, 3, 3, 4),
                           y = c(2, 3, 1, 3, 1, 2))

wave_1_start = "2020-03-20"
wave_1_end = "2020-05-29"
wave_2_start = "2020-09-30"
wave_2_end = "2021-02-12"


# ---- Load data -----------------------------------------------
demo <- dbGetQuery(con, "SELECT a.person_id_deid, a.sex, a.ethnic_group, 2021 - left(a.dob,4) as age, a.IMD_quintile FROM dars_nic_391419_j3w9t_collab.ccu013_master_demographics a 
INNER JOIN (SELECT NHS_NUMBER_DEID FROM dars_nic_391419_j3w9t_collab.ccu013_dp_skinny_patient_23_01_2020) b
ON a.person_id_deid = b.NHS_NUMBER_DEID")
nrow(demo)  # 57032174

age <- dbGetQuery(con, "SELECT
                      person_id_deid, age
                    FROM 
                      dars_nic_391419_j3w9t_collab.ccu013_paper_table_one_56million_denominator")
                  



#-- Find trajectories for ALL individuals
#----------------------------------------------------------------

# -- if already calculated read in here for speed!
alreadycalculated <- T
if(alreadycalculated){
  uniid <- read.table(paste0(folder_path, basefilename, "_individual_trajectories.txt"), header =T, sep = "\t")
  if("covid_severity" %in% names(uniid)){uniid$covid_severity <- NULL }
}else{
  # Note that the data has already been correctly ordered by person_id_deid, date and the specified order of phenotypes in the notebook CCU013_13_paper_subset_data_to_cohort. 
  # Note we are only selecting the samples who actually have events, the rest can be added afterwards!
  tdata <- dbGetQuery(con, "SELECT a.person_id_deid, a.date, a.trajectory_phenotype, a.phenotype_order, a.days_passed 
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_icu as a
INNER JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as b
ON a.person_id_deid == b.person_id_deid")
  uniid <- find_trajectories(tdata)
  write.table(uniid, paste0(folder_path, basefilename, "_individual_trajectories.txt"), sep = "\t", row.names = F, quote = F)
}

## Add on individuals who did not get infected
unaffected <- dbGetQuery(con, "SELECT a.person_id_deid, 'Unaffected' as trajectory, NULL as days 
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_icu as a
LEFT ANTI JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as b
ON a.person_id_deid == b.person_id_deid")
uniid <- rbind(uniid, unaffected)
n_unaffected <- sum(is.na(uniid$days))

# update tdata to include all
tdata <- dbGetQuery(con, "SELECT person_id_deid, date, trajectory_phenotype, phenotype_order, days_passed 
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_icu")

#-- All ----------------------------------------------------------
# Find and create trajectory plots
create_trajectory_plot(uniid,
                       tdata, # Used to calculate size of nodes and number of phenotypes!
                       folder_path, 
                       basefilename, 
                       prefix_file_name = "all",
                       subtitle_on_plot = "All",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)


#--- WAVE 1 -----------------------------------------------------------------------------------------
## Load the wave1 subset produced in notebook: CCU013_13_paper_subset_data_to_cohort
### Only include people who where effected
alreadycalculated_wave1 <- T
if(alreadycalculated_wave1){
  uniid_wave1 <- read.table(paste0(folder_path, basefilename, "_individual_trajectories_wave1.txt"), header =T, sep = "\t")
  if("covid_severity" %in% names(uniid_wave1)){uniid_wave1$covid_severity <- NULL }
}else{
  tdata_wave1 <- dbGetQuery(con, "SELECT a.person_id_deid, a.date, a.trajectory_phenotype, a.phenotype_order, a.days_passed 
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave1_icu as a
INNER JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave1 as b
ON a.person_id_deid == b.person_id_deid")
  uniid_wave1 <- find_trajectories(tdata_wave1)
  write.table(uniid_wave1, paste0(folder_path, basefilename, "_individual_trajectories_wave1.txt"), sep = "\t", row.names = F, quote = F)
}

## Add on individuals who did not get infected
unaffected <- dbGetQuery(con, "SELECT DISTINCT a.person_id_deid, 'Unaffected' as trajectory, NULL as days 
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave1_icu as a
LEFT ANTI JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave1 as b
ON a.person_id_deid == b.person_id_deid")
uniid_wave1 <- rbind(uniid_wave1, unaffected)

# update tdata to include all
tdata_wave1 <- dbGetQuery(con, "SELECT person_id_deid, date, trajectory_phenotype, phenotype_order, days_passed 
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave1_icu")

# Find and create trajectory plots
n_unaffected <- sum(is.na(uniid_wave1$days))
create_trajectory_plot(uniid_wave1, tdata_wave1,
                       folder_path, 
                       basefilename, 
                       prefix_file_name = "wave1",
                       subtitle_on_plot = "Wave 1",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#--- WAVE 2 -----------------------------------------------------------------------------------------

## Load the wave1 subset produced in notebook: CCU013_13_paper_subset_data_to_cohort
alreadycalculated_wave2 <- T
if(alreadycalculated_wave2){
  uniid_wave2 <- read.table(paste0(folder_path, basefilename, "_individual_trajectories_wave2.txt"), header =T, sep = "\t")
  if("covid_severity" %in% names(uniid_wave2)){uniid_wave2$covid_severity <- NULL }
}else{
  ### Only include people who where effected
  tdata_wave2 <- dbGetQuery(con, "SELECT a.person_id_deid, a.date, a.trajectory_phenotype, a.phenotype_order, a.days_passed 
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave2_icu as a
INNER JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave2 as b
ON a.person_id_deid == b.person_id_deid")
  uniid_wave2 <- find_trajectories(tdata_wave2)
  write.table(uniid_wave2, paste0(folder_path, basefilename, "_individual_trajectories_wave2.txt"), sep = "\t", row.names = F, quote = F)
}

## Add on individuals who did not get infected
unaffected <- dbGetQuery(con, "SELECT DISTINCT a.person_id_deid, 'Unaffected' as trajectory, NULL as days 
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave2_icu as a
LEFT ANTI JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort_wave2 as b
ON a.person_id_deid == b.person_id_deid")
uniid_wave2 <- rbind(uniid_wave2, unaffected)

# update tdata to include all
tdata_wave2 <- dbGetQuery(con, "SELECT a.person_id_deid, a.date, a.trajectory_phenotype, a.phenotype_order, a.days_passed, b.vaccine_status
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave2_icu AS a
LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_wave2_vax_status AS b
on a.person_id_deid = b.person_id_deid")

# Find and create trajectory plots
n_unaffected <- sum(is.na(uniid_wave2$days))
create_trajectory_plot(uniid_wave2, tdata_wave2, 
                       folder_path, 
                       basefilename, 
                       prefix_file_name = "wave2",
                       subtitle_on_plot = "Wave 2",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Wave 2 - Vaccinated -----------------------------------------------------------------------------
tdata_wave2_vax <- dbGetQuery(con, "SELECT a.person_id_deid, a.date, a.trajectory_phenotype, a.phenotype_order, a.days_passed, b.vaccine_status
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave2_icu AS a
LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_wave2_vax_status AS b
on a.person_id_deid = b.person_id_deid WHERE vaccine_status == \"Vaccinated\"")
uniid_wave2_vax <- uniid_wave2[which(uniid_wave2[,1] %in% tdata_wave2_vax[,1]), ]


# Find and create trajectory plots
n_unaffected <- sum(is.na(uniid_wave2_vax$days))
create_trajectory_plot(uniid_wave2_vax, tdata_wave2_vax, 
                       folder_path, 
                       basefilename, 
                       prefix_file_name = "wave2_vax",
                       subtitle_on_plot = "Wave 2: Vaccinated",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.00001,
                       n_unaffected)


#-- Wave 2 - Unvaccinated ----------------------------------------------------------
tdata_wave2_unvax <- dbGetQuery(con, "SELECT a.person_id_deid, a.date, a.trajectory_phenotype, a.phenotype_order, a.days_passed, b.vaccine_status
FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data_wave2_icu AS a
LEFT JOIN dars_nic_391419_j3w9t_collab.ccu013_no_covid_before_wave2_vax_status AS b
on a.person_id_deid = b.person_id_deid WHERE vaccine_status == \"Unvaccinated\"")
uniid_wave2_unvax <- uniid_wave2[which(uniid_wave2[,1] %in% tdata_wave2_unvax[,1]), ]


# Find and create trajectory plots
n_unaffected <- sum(is.na(uniid_wave2_unvax$days))
create_trajectory_plot(uniid_wave2_unvax, tdata_wave2_unvax, 
                       folder_path, 
                       basefilename, 
                       prefix_file_name = "wave2_unvax",
                       subtitle_on_plot = "Wave 2: Unvaccinated",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Male -----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$sex ==1), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 28279784

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "male",
                       subtitle_on_plot = "Male",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Female ---------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$sex ==2), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 28752327

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "female",
                       subtitle_on_plot = "Female",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Age under 18th -------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% age[which(age$age<18), "person_id_deid"]),]
#tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$age<18), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 11612932

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "age_under_18",
                       subtitle_on_plot = "Age under 18",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = F, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Age 18-29 ------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% age[which(age$age>=18 & age$age <=29), "person_id_deid"]),]
#tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$age>=18 & demo$age <=29), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 8199384

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "age_18-29",
                       subtitle_on_plot = "Age 18 - 29",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = F, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Age 30-49 ------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% age[which(age$age>=30 & age$age <=49), "person_id_deid"]),]
#tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$age>=30 & demo$age <=49), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 15665505

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "age_30-49",
                       subtitle_on_plot = "Age 30 - 49",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = F, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)
#-- Age 50-69 ------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% age[which(age$age>=50 & age$age <=69), "person_id_deid"]),]
#tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$age>=50 & demo$age <=69), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 13987885

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "age_50-69",
                       subtitle_on_plot = "Age 50 - 69",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = F, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)


#-- Age Over 70 ----------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% age[which(age$age>=70), "person_id_deid"]),]
#tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$age>=70), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 8324658

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "age_70_above",
                       subtitle_on_plot = "Age 70 or above",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = F, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- White ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "White"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 37853035

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_white",
                       subtitle_on_plot = "Ethnicity white",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Asian or Asian British -----------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Asian or Asian British"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_asian",
                       subtitle_on_plot = "Ethnicity Asian or Asian British",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Black or Black british -----------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Black or Black British"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 137809

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_black",
                       subtitle_on_plot = "Ethnicity Black or Black British",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Chinese --------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Chinese"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 10409

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_chinese",
                       subtitle_on_plot = "Ethnicity Chinese",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Mixed ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Mixed"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 68320

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_mixed",
                       subtitle_on_plot = "Ethnicity mixed",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- Other ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Other"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 74554

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_other",
                       subtitle_on_plot = "Ethnicity Other",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- IMD 1 ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$IMD_quintile == 1), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 831118

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "imd_1",
                       subtitle_on_plot = "IMD quantile 1",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)

#-- IMD 5 ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$IMD_quintile == 5), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 559499

n_unaffected <- sum(is.na(uniid_sub$days))
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "imd_5",
                       subtitle_on_plot = "IMD quantile 5",
                       graph_layout, 
                       plot_days = T,
                       N_total_pop,
                       pre_processing_done = T, # If this function has been run once set this to True to speed up
                       threshold = 0.0001,
                       n_unaffected)


