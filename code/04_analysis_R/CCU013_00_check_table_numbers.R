# ---- R script to do integrety checks on main table output by CCU013 COVID-19 severity phenotyping -----------
# Tables checked: ccu013_covid_severity OR ccu013_covid_severity_paper_cohort
#                 ccu013_covid_trajectory OR ccu013_covid_trajectory_paper_cohort
# Author: Johan Hilge Thygesen
# Last updated: 16.06.21


# ---- Load tables ------------------
severity <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity")
traject <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory")
deaths <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_deaths_frzon28may_mm_210528 as d
                     RIGHT JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory as t on d.DEC_CONF_NHS_NUMBER_CLEAN_DEID = t.person_id_deid
                     WHERE t.covid_phenotype = '04_Fatal_without_covid_diagnosis'")

severity <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity_paper_cohort")
traject <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort")
deaths <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_deaths_frzon28may_mm_210528 as d
                     RIGHT JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as t on d.DEC_CONF_NHS_NUMBER_CLEAN_DEID = t.person_id_deid
                     WHERE t.covid_phenotype = '04_Fatal_without_covid_diagnosis'")

start_date = '2020-01-01' # None paper cohort
start_date = '2020-01-23'
end_date = '2021-07-29'


# ----- Start checks

nrow(severity) # 3469528 - for paper_cohort 
nrow(traject) # 8825738 - for paper_cohort 
nrow(unique(traject)) # 8825738 - for paper_cohort 

## 1) Check that all samples have identifies
all(!is.na(severity[,1]))
all(!is.na(traject[,1]))
head(traject[which(is.na(traject[,1])),])
table(traject[which(is.na(traject[,1])),"source"])
head(severity[which(is.na(severity[,1])),])

## 2) Check for duplicates in severity table
test <- as.data.frame(table(severity[,1]))
test <- test[which(test[,2]>1),]
print(paste0("Duplicates in severity table: ", nrow(test)))
## Check out some duplicates if they are there
if(nrow(test)>0){
  severity[which(severity[,1]==test[4,1]),]
  example <- traject[which(traject[,1]==test[4,1]),]
  example <- example[order(example[,"date"]),]
  example
}

## 3) Check if all ids in the trajectory table are in the severity table
length(unique(severity$person_id_deid))
length(unique(traject$person_id_deid))
all(traject[,1] %in% severity[,1])
missing <- traject[-which(traject[,1] %in% severity[,1]),]
print(paste0("IDs in trajectory table and not in severity table!: ", nrow(missing)))
## Check out whats missing
if(nrow(missing)>0){
  all(is.na(missing[,1]))
  table(missing$covid_phenotype)
}
if(nrow(missing)>0){
  head(missing)
  example <- trejectory[which(trejectory[,1]==test[4,1]),]
  example <- example[order(example[,"date"]),]
  example
}

## 4) Check that no dates are higher than or below the given
if(any(severity[,'date']<as.Date(start_date, format = "%Y-%m-%d"))){stop("Dates below start_date found in Severity table!")}
if(any(severity[,'date']>as.Date(end_date, format = "%Y-%m-%d"))){stop("Dates after end_date found in Severity table!")}
if(any(traject[,'date']<as.Date(start_date, format = "%Y-%m-%d"), na.rm = T)){stop("Dates below start_date found in Trajectory table!")}
if(any(traject[,'date']>as.Date(end_date, format = "%Y-%m-%d"), na.rm = T)){stop("Dates after end_date found in Trajectory table!")}
head(traject[which(traject[,'date']<as.Date(start_date, format = "%Y-%m-%d")),])
table(traject[which(traject[,'date']<as.Date(start_date, format = "%Y-%m-%d")),"source"])
nrow(traject[which(traject[,'date']<as.Date(start_date, format = "%Y-%m-%d")),])

## 5) Check for overlap between death with and without diagnosis
withDiag <- unique(traject[which(traject[,"covid_phenotype"] == '04_Fatal_with_covid_diagnosis'), "person_id_deid"])
withOutDiag <- unique(traject[which(traject[,"covid_phenotype"] == '04_Fatal_without_covid_diagnosis'), "person_id_deid"])
if(any(withDiag %in% withOutDiag)){
  print("Overlap between death with and without diagnosis found!")
  print("Size of overlap: ", length(withDiag[withDiag%in%withOutDiag]))
}

## 6) Any dates are NA
if(any(is.na(traject[,"date"]))){
  print(paste0(sum(is.na(traject[,"date"])), " NA date fields found!"))
  head(traject[is.na(traject[,"date"]),])
}


## 7) Any deats without COVID has COVID diag?
#table(severity[which(severity[,"person_id_deid"]%in%deaths[,"person_id_deid"]),"covid_severity"])
#table(traject[which(traject[,"person_id_deid"]%in%deaths[,"person_id_deid"]),"covid_phenotype"])
icdcols <- grep("_COD", names(deaths), value = T)
icdcols <- icdcols[5:length(icdcols)]
for(i in 1:length(icdcols)){
  if(any(deaths[,icdcols[i]] %in% c("U071", "U072"))){print(
    paste0("Deaths from covid found in patients wihout covid on certificate, death col:", icdcols[i]))}
}
head(icdcols)
#head(deaths)
#deaths[which(deaths[,"person_id_deid"]=="3DQOF5766RUCLZA"),]
#traject[which(traject[,1]=="07O8FWQ7ZTV038C"),]

# ## Multiple death dates
# test <- traject[which(traject$covid_phenotype %in% c("04_Fatal_with_covid_diagnosis","04_Fatal_without_covid_diagnosis")),]
# test2 <- as.data.frame(table(test[,1]))
# test2 <- test2[which(test2[,2]>1),]
# test2 <- test2[order(test2[,2], decreasing = T),]
# nrow(test2)
# head(test2)
# table(test2[,2])
# test[which(test[,1]== "001WF09LDQHE1X3"),]
# test[which(test[,1]== "05LTERN3NH4C7P5"),]
# traject[which(traject[,1]== "05LTERN3NH4C7P5"),]
# 
# 
# # Number of events per id
# test <- unique(traject[,c("person_id_deid", "covid_phenotype")])
# table(test$covid_phenotype)
# 
# # Number of confirmed cases
# length(unique(traject[which(traject$covid_status == "confirmed"),"person_id_deid"]))
# 
# # Number of positive tests
# length(unique(traject[which(traject$covid_phenotype == "01_Covid_positive"),"person_id_deid"]))



