## R script to create patient trajectory network plots
#    - Plots used for figures in the COVID-Phenotype severity paper
# Authors: Johan Hilge Thygesen
# Last updated: 17.06.21

library(igraph)
library(scales)
library(dplyr)
source("~/dars_nic_391419_j3w9t_collab/CCU013/04_analysis_R/CCU013_colorscheme.R")
source("~/dars_nic_391419_j3w9t_collab/CCU013/04_analysis_R/CCU013_trejectory_plot_function.R")
# ---- Load data -----------------------------------------------

# Note that the data has already been correctly ordered by person_id_deid, date and the specified order of phenotypes in the notebook. 
tdata <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data")
nrow(tdata) # 5997870

demo <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort")
nrow(demo)  # 3454653

cohort = dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_events_demographics_paper_cohort")
nrow(cohort)

folder_path <- "~/dars_nic_391419_j3w9t_collab/CCU013/output/trajectories"
basefilename <- "CCU013_trajectories"

# X and Y coordinates for phenotype node location on plot - note values are relative to each other, i.e. coordinate are not used as absolute values!
graph_layout <- data.frame(phenotype = c("Positive test", "GP Diagnosis", "Hospitalisation", "Critical care", "Death"),
                           x = c(1, 1, 2, 2, 3),
                           y = c(3, 1, 3.5, 0.5, 2))

#-- Find trajectories for all individuals

# -- if already calculated read in here for speed!
# uniid <- read.table(paste0(folder_path, basefilename, "_individual_trajectories.txt"), header =T, sep = "\t")

# else
uniid <- find_trajectories(tdata)
write.table(uniid, paste0(folder_path, basefilename, "_individual_trajectories.txt"), sep = "\t", row.names = F, quote = F)

#-- All ----------------------------------------------------------
# Find and create trajectory plots
create_trajectory_plot(uniid,
                       tdata, 
                       folder_path, 
                       basefilename, 
                       prefix_file_name = "all",
                       subtitle_on_plot = "All",
                       graph_layout, 
                       plot_days = T)


#-- SUBSET data set for interest and run plot function
#-- Identify wave cohorts------------------------------------------------------------------

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

#-- Wave 1 ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% cohort[which(cohort[,"wave"]==1), "person_id_deid"]), ]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]

#summary(tdata_sub$date)
#ggplot(tdata_sub, aes(date)) + geom_histogram(bins = 100)+
#geom_vline(xintercept=as.Date("2020-03-20"), linetype="dashed", color="blue", size=0.3) +
#  geom_vline(xintercept=as.Date("2020-03-26"), linetype="dashed", color="red", size=0.2) + 
#  geom_vline(xintercept=as.Date("2020-05-29"), linetype="dashed", color="blue", size=0.3)

length(unique(tdata_sub$person_id_deid)) # 250639

# Find and create trajectory plots
create_trajectory_plot(uniid_sub, tdata_sub, 
                       folder_path, 
                       basefilename, 
                       prefix_file_name = "wave1",
                       subtitle_on_plot = "Wave 1",
                       graph_layout, 
                       plot_days = T)

#-- Wave 2 ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% cohort[which(cohort[,"wave"]==2), "person_id_deid"]), ]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]

summary(tdata_sub$date)
ggplot(tdata_sub, aes(date)) + geom_histogram(bins = 100)+
  geom_vline(xintercept=as.Date("2020-09-30"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-10-31"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-01-06"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-02-12"), linetype="dashed", color="blue", size=0.3)  

length(unique(tdata_sub$person_id_deid)) # 2808763

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "wave2",
                       subtitle_on_plot = "Wave 2",
                       graph_layout, 
                       plot_days = T)


#-- Male -----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$sex ==1), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 1561838

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "male",
                       subtitle_on_plot = "Male",
                       graph_layout, 
                       plot_days = T)

#-- Female ---------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$sex ==2), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 1892815

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "female",
                       subtitle_on_plot = "Female",
                       graph_layout, 
                       plot_days = T)

#-- Age under 18th -------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$age<18), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 226009
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "age_under_18",
                       subtitle_on_plot = "Age under 18",
                       graph_layout, 
                       plot_days = T)



#-- Age 18-29 ------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$age>=18 & demo$age <=29), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 702988
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "age_18-29",
                       subtitle_on_plot = "Age 18 - 29",
                       graph_layout, 
                       plot_days = T)

#-- Age 30-49 ------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$age>=30 & demo$age <=49), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 1140771
create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "age_30-49",
                       subtitle_on_plot = "Age 30 - 49",
                       graph_layout, 
                       plot_days = T)
#-- Age 50-69 ------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$age>=50 & demo$age <=69), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 1140771

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "age_50-69",
                       subtitle_on_plot = "Age 50 - 69",
                       graph_layout, 
                       plot_days = T)


#-- Age Over 70 ----------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$age>=70), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 469501

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "age_70_above",
                       subtitle_on_plot = "Age 70 or above",
                       graph_layout, 
                       plot_days = T)

#-- White ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "White"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 2660844

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_white",
                       subtitle_on_plot = "Ethnicity white",
                       graph_layout, 
                       plot_days = T)

#-- Asian or Asian British -----------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Asian or Asian British"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 445702

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_asian",
                       subtitle_on_plot = "Ethnicity Asian or Asian British",
                       graph_layout, 
                       plot_days = T)

#-- Black or Black british -----------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Black or Black British"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 137809

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_black",
                       subtitle_on_plot = "Ethnicity Black or Black British",
                       graph_layout, 
                       plot_days = T)

#-- Chinese --------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Chinese"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 10409

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_chinese",
                       subtitle_on_plot = "Ethnicity Chinese",
                       graph_layout, 
                       plot_days = T)

#-- Mixed ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Mixed"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 68320

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_mixed",
                       subtitle_on_plot = "Ethnicity mixed",
                       graph_layout, 
                       plot_days = T)

#-- Other ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$ethnic_group == "Other"), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 74554

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "ethnicity_other",
                       subtitle_on_plot = "Ethnicity Other",
                       graph_layout, 
                       plot_days = T)

#-- IMD 1 ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$imd_quintile == 1), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 831118

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "imd_1",
                       subtitle_on_plot = "IMD quantile 1",
                       graph_layout, 
                       plot_days = T)

#-- IMD 5 ----------------------------------------------------------
tdata_sub <- tdata[which(tdata[,"person_id_deid"] %in% demo[which(demo$imd_quintile == 5), "person_id_deid"]),]
uniid_sub <- uniid[which(uniid[,"person_id_deid"] %in% tdata_sub[,"person_id_deid"]), ]
length(unique(tdata_sub$person_id_deid)) # 559499

create_trajectory_plot(uniid_sub, tdata_sub, folder_path, basefilename, 
                       prefix_file_name = "imd_5",
                       subtitle_on_plot = "IMD quantile 5",
                       graph_layout, 
                       plot_days = T)


