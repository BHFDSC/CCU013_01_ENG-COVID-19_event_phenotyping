## R script to create time line histogram/density of COVID-19 events
#    - Plots used as figures in the COVID-Phenotype severity paper
# Authors: Johan Hilge Thygesen & Chris Tomlinson
# Last updated: 23.01.22

library("ggplot2")
library("stringr")
source("~/dars_nic_391419_j3w9t_collab/CCU013/04_analysis_R-GITHUB-v3/CCU013_colorscheme.R")


# Config ------------------------------------------------------------------

start_date = '2020-01-23'
end_date = '2021-11-30'
traject <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort")

# Pre-processing ----------------------------------------------------------

# Remove dates: NA, < start
if(any(is.na(traject[,"date"]))){traject <- traject[-which(is.na(traject[,'date'])),]}
if(any(traject[,"date"]<start_date)){traject <- traject[-which(traject[,'date']<start_date),]}
if(any(traject[,"date"]>end_date)){traject <- traject[-which(traject[,'date']>end_date),]}

tdate <- unique(traject[,c("person_id_deid", "covid_phenotype", "date")])

## Add in vaccination data
vax <- dbGetQuery(con, "SELECT distinct person_id_deid, dose2 FROM dars_nic_391419_j3w9t_collab.ccu013_vaccine_status_paper_cohort")
vax[,"covid_phenotype"] <- "Vaccination - 2nd dose"
colnames(vax) <- c("person_id_deid", "date", "covid_phenotype")
tdate <- rbind(tdate, vax)



# Renaming ----------------------------------------------------------------
# _ to space and dash after number
tdate[, "covid_phenotype"] <- gsub("01_", "", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("02_", "", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("03_", "", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("04_", "", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("_", " ", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("Covid positive test", "Positive test", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("GP covid diagnosis", "Primary care diag.", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("Covid admission", "Hospital admission", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("Fatal with covid diagnosis", "Fatal with diagnosis", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("Fatal without covid diagnosis", "Fatal no diagnosis", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("Covid inpatient death", "Inpatient death", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("Vaccination - 2nd dose", "Vaccination", tdate[, "covid_phenotype"])

# Chris rename again during releveling
unique(tdate[, "covid_phenotype"])
tdate[,"covid_phenotype"] <- factor(tdate[,"covid_phenotype"], levels = c("Positive test", "Primary care diag.",
                                                                          "Hospital admission",
                                                                          "NIV treatment", "IMV treatment",
                                                                          "ECMO treatment", "ICU admission", "Fatal with diagnosis",
                                                                          "Fatal no diagnosis", "Inpatient death", "Vaccination"))

mycolors2 <- c("#74c476",
              "#006d2c", 
              "#fe9929",
              "#ff6800",
              "#ff6800",
              "#ff6800",
              "#ff6800",
              "#b3cde3",
              "#b3cde3",
              "#b3cde3",
              "#B8B8B8")

p3 <- ggplot(tdate, aes(x = date, fill = covid_phenotype)) + geom_density(aes(y = ..count..), alpha = 0.75) + xlab("") + ylab("") + 
  facet_grid(rows = vars(covid_phenotype), scales = "free") + scale_fill_manual(values = mycolors2) + 
  theme_bw() +  scale_x_date(date_breaks = "2 month", date_labels = "%b %Y") + 
  coord_cartesian(xlim = c(as.Date("2020-02-23"), as.Date("2021-09-25"))) + 
  theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust = 0.5), 
        strip.text = element_text(size = 5), legend.position = "None") +
  # Lockdown and wave definitions
  geom_vline(xintercept=as.Date("2020-03-20"), linetype="dashed", color="blue", size=0.3) +
  geom_vline(xintercept=as.Date("2020-03-26"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2020-05-29"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-09-30"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-10-31"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-01-06"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-02-12"), linetype="dashed", color="blue", size=0.3) +
  geom_vline(xintercept=as.Date("2021-02-01"), linetype="dashed", color="black", size=0.5)
p3

# Supplementary figure 4: Source Timeline Histograms --------------------------------------

sdate <- unique(traject[,c("person_id_deid", "source", "date")])

## Add vaccination data to sources
vax[,"source"] <- "vaccine"
sdate <- rbind(sdate, unique(vax[,c("person_id_deid", "date", "source")]))
sdate[,"source"] <- factor(sdate[,"source"], levels = c("SGSS","GDPPR", "HES APC", "CHESS", "SUS", "HES CC", "deaths", "vaccine"))


mycolors3 <- c("#74c476",
               "#006d2c", 
               "#fe9929",
               "#fe9929",
               "#fe9929",
               "#fe9929",
               "#b3cde3",
               "#B8B8B8")

p4 <- ggplot(sdate, aes(x = date, fill = source)) + geom_density(aes(y = ..count..), alpha = 0.75) + xlab("") + ylab("") + 
  facet_grid(rows = vars(source), scales = "free") +  theme_bw() +  scale_x_date(date_breaks = "2 month", date_labels = "%b %Y") + 
  scale_fill_manual(values = mycolors3) + 
  coord_cartesian(xlim = c(as.Date("2020-02-23"), as.Date("2021-09-25"))) + 
  theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust = 0.5), 
        strip.text = element_text(size = 6), legend.position = "None")+
  # Lockdown and wave definitions
  geom_vline(xintercept=as.Date("2020-03-20"), linetype="dashed", color="blue", size=0.3) +
  geom_vline(xintercept=as.Date("2020-03-26"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2020-05-29"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-09-30"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-10-31"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-01-06"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-02-12"), linetype="dashed", color="blue", size=0.3) +   
  geom_vline(xintercept=as.Date("2021-02-01"), linetype="dashed", color="black", size=0.5)
p4


# Export ------------------------------------------------------------------
pdf("~/dars_nic_391419_j3w9t_collab/CCU013/output/figures/CCU013_supl-fig3_timeline_pheno.pdf", height = 9, width = 7)
p3
dev.off()

pdf("~/dars_nic_391419_j3w9t_collab/CCU013/output/figures/CCU013_supl-fig4_timeline_source.pdf", height = 9, width = 7)
p4
dev.off()
