library(dplyr)

## Script to produce numbers for figure 1 - Flow chart of phenotyping

traject <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort")
nrow(severity) # 3469528

## N individuals
test <- unique(traject[,c("person_id_deid", "covid_phenotype")])
n_pheno <- as.data.frame(table(test$covid_phenotype))
colnames(n_pheno) <- c("group", "n_individuals")

## N observations
n_obs <- as.data.frame(table(traject$covid_phenotype))
colnames(n_obs) <- c("group", "n_obs")

fig1 <- merge(n_pheno, n_obs, by = "group", all.x = T)
fig1[,"individual_percent"] <- paste0(round(fig1[,"n_individuals"] / nrow(severity) * 100,1), "%")
head(fig1)

## N severity
traject[which(traject$covid_phenotype %in% c("01_GP_covid_diagnosis", "01_Covid_positive_test")), "severity_index"] <- 1
traject[which(traject$covid_phenotype %in% c("01_GP_covid_diagnosis", "01_Covid_positive_test")), "severity"] <- "Not hospitalised"
traject[which(traject$covid_phenotype %in% c("02_Covid_admission")), "severity_index"] <- 2
traject[which(traject$covid_phenotype %in% c("02_Covid_admission")), "severity"] <- "Hospitalised"
traject[which(traject$covid_phenotype %in% c("03_ICU_admission", "03_ECMO_treatment", "03_IMV_treatment", "03_NIV_treatment")), "severity_index"] <- 3
traject[which(traject$covid_phenotype %in% c("03_ICU_admission", "03_ECMO_treatment", "03_IMV_treatment", "03_NIV_treatment")), "severity"] <- "Critical Care"
as.data.frame(table(severity$severity))

# Get max severity
severity <- unique(traject[,c("person_id_deid", "severity_index", "covid_phenotype", "severity")]) %>%
  group_by(person_id_deid) %>%
  arrange(desc(severity_index)) %>%
  slice(1L)
severity <- as.data.frame(severity)
as.data.frame(table(severity$severity))

# All NA's represent death only
any(is.na(severity$severity))
table(severity[which(is.na(severity$severity)),"covid_phenotype"])
severity[which(is.na(severity$severity)),"severity"] <- "Death only"

sever <- as.data.frame(table(severity$severity))
colnames(sever) <- c("group", "n_individuals")
sever[,"n_obs"] <- NA
sever[,"individual_percent"] <- paste0(round(sever[,"n_individuals"] / nrow(severity) * 100,1), "%")

fig1 <- rbind(sever, fig1)

write.table(fig1, "~/dars_nic_391419_j3w9t_collab/CCU013/output/figures/CCU013_fig1_numbers.txt", row.names = F, quote = F, sep = "\t")
