# R script to create supplementary Table5: Primary diagnosis on death certificate for decesed patients
# Included in the COVID-19 severity phenotyping paper
# Author: Johan Hilge Thygesen
# Last modified 17.06.21

library(reshape2)

# --- Load and subset data sources ---------------------------------------------
traject <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort")
death_diags <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_deaths")

covid_pheno_death <- unique(traject[which(traject[,"covid_phenotype"] %in% c("04_Fatal_with_covid_diagnosis",
                                                                      "04_Fatal_without_covid_diagnosis")),c("person_id_deid", "covid_phenotype")])
deaths <- death_diags[which(death_diags[,"person_id_deid"] %in% covid_pheno_death[,"person_id_deid"]), ]

# Only primary diagnosis
deaths <- deaths[,c("person_id_deid", "S_UNDERLYING_COD_ICD10")]

# Include sub diagnosis
#deaths <- deaths[,c("person_id_deid", "S_UNDERLYING_COD_ICD10", "S_COD_CODE_1", "S_COD_CODE_2", "S_COD_CODE_3", "S_COD_CODE_4", "S_COD_CODE_5", "S_COD_CODE_6", "S_COD_CODE_7",
#                    "S_COD_CODE_8", "S_COD_CODE_9", "S_COD_CODE_10", "S_COD_CODE_11", "S_COD_CODE_12", "S_COD_CODE_13", "S_COD_CODE_14", "S_COD_CODE_15")]

# Make long-list of death diagnosis
dm <- melt(deaths, id.vars = "person_id_deid") 
dm <- unique(dm[,c("person_id_deid", "value")])

# Per individual remove missing diagnosis if another is present
test <- as.data.frame(table(dm$person_id_deid))
test <- test[which(test[,2]>1),]
for(i in 1:nrow(test)){
  dm[which(dm[,1]==test[i,1] & is.na(dm[,"value"])),"remove"] <- T
  print(nrow(test) - i)
}
nrow(dm)
dm <- dm[-which(dm$remove == T), ]
nrow(dm)

# Merge data
dm <- merge(dm, covid_pheno_death, by = "person_id_deid", all.x = T)
nrow(dm)

head(dm)
incl_diag <- 10 # how many diagnosis to display

#-- Find most frequent diagnosis for id's dying without diagnosis ---------------------------------------
topdiag <- head(sort(table(dm[which(dm[,"covid_phenotype"] == "04_Fatal_without_covid_diagnosis"), "value"], useNA = "ifany"),decreasing = T), incl_diag)
n_diags <- nrow(dm[which(dm[,"covid_phenotype"] == "04_Fatal_without_covid_diagnosis"), ])
n_ids <- length(unique(traject[which(traject[,"covid_phenotype"] == "04_Fatal_without_covid_diagnosis"), "person_id_deid"]))
out1 <- as.data.frame(topdiag)
out1[,"percent"] <- round(out1[,"Freq"] / n_ids * 100, 3)
out1[,"covid_phenotype"] <- "04_Fatal_without_covid_diagnosis"
out1[nrow(out1)+1, "covid_phenotype"] <- paste0(n_diags, " pimary diagnosis in ", n_ids, " individuals")

#-- Find most frequent diagnosis for id's dying with diagnosis ------------------------------------------
n_diags <- nrow(dm[which(dm[,"covid_phenotype"] == "04_Fatal_with_covid_diagnosis"), ])
n_ids <- length(unique(traject[which(traject[,"covid_phenotype"] == "04_Fatal_with_covid_diagnosis"), "person_id_deid"]))
topdiag <-  head(sort(table(dm[which(dm[,"covid_phenotype"] == "04_Fatal_with_covid_diagnosis"), "value"], useNA = "ifany"),decreasing = T), incl_diag)
out2 <- as.data.frame(topdiag)
out2[,"percent"] <- round(out2[,"Freq"] / n_ids * 100, 3)
out2[,"covid_phenotype"] <- "04_Fatal_with_covid_diagnosis"
out2[nrow(out2)+1, "covid_phenotype"] <- paste0(n_diags, " pimary diagnosis in ", n_ids, " individuals")

#-- Save output
out <- rbind(out2, out1)
write.table(out, "~/dars_nic_391419_j3w9t_collab/CCU013/output/CCU013_most_frequent_death_diagnosis.txt", sep = "\t", quote = F, row.names = F)
