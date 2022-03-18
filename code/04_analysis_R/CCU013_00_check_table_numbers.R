# ---- R script to do integrety checks on main table output by CCU013 COVID-19 severity phenotyping -----------
# Tables checked: ccu013_covid_severity OR ccu013_covid_severity_paper_cohort
#                 ccu013_covid_trajectory OR ccu013_covid_trajectory_paper_cohort
# Author: Johan Hilge Thygesen
# Last updated: 22.01.22


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
end_date = '2021-11-30'


# ----- Start checks

# Old Medxriv paper cohort number
# severity: 3469528 
# trajectory: 8825738
# unique(trajectory): 8825738

nrow(severity) # Cohort 7,244,925 |8,714,594 Full Sample
nrow(traject) # Cohort 13,990,425 | 15,825,444 Full Sample
nrow(unique(traject)) # Cohort 13,990,425 | 15825444 Full Sample  

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
head(severity[which(severity[,'date']<as.Date(start_date, format = "%Y-%m-%d")),])
nrow(severity[which(severity[,'date']<as.Date(start_date, format = "%Y-%m-%d")),])


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

################################
### Additional checks for the PAPER_COHORT ONLY

### 8) Check that everyone with less than 28 days followup time dies! - otherwise the followup time is not working

## Find all ids who have less than 28 days followup time
myquery <- paste0("select distinct t.person_id_deid, first_event, date, covid_phenotype FROM (
  Select person_id_deid, first_event FROM 
  (SELECT person_id_deid, min(date) as first_event FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort
    group by person_id_deid)
    WHERE first_event > \"", as.character(as.Date(end_date) - 28) ,"\") as f
   INNER JOIN dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort as t
   ON f.person_id_deid == t.person_id_deid
   order by t.person_id_deid")
first_event <- dbGetQuery(con, myquery)

## How many with 28 days followup time?
length(unique(first_event$person_id_deid))

## How many of these die die
first_event_death <- first_event[which(strtrim(first_event$covid_phenotype,2) == "04"),]
table(first_event_death$covid_phenotype)
length(unique(first_event_death$person_id_deid))

## All okay
length(unique(first_event$person_id_deid))==length(unique(first_event_death$person_id_deid))


