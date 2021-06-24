## R script to create time line histogram/density of COVID-19 events
#    - Plots used as figures in the COVID-Phenotype severity paper
# Authors: Johan Hilge Thygesen & Chris Tomlinson
# Last updated: 16.06.21

library("ggplot2")
library("stringr")
source("CCU013_colorscheme.R")
#library("ggridges")


# Config ------------------------------------------------------------------

start_date = '2020-01-23'
end_date = '2021-03-31'
traject <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort")

# Pre-processing ----------------------------------------------------------

# Remove dates: NA, < start
if(any(is.na(traject[,"date"]))){traject <- traject[-which(is.na(traject[,'date'])),]}
if(any(traject[,"date"]<start_date)){traject <- traject[-which(traject[,'date']<start_date),]}
if(any(traject[,"date"]>end_date)){traject <- traject[-which(traject[,'date']>end_date),]}

tdate <- unique(traject[,c("person_id_deid", "covid_phenotype", "date")])
sort(table(tdate$covid_phenotype), decreasing = T)


# Renaming ----------------------------------------------------------------

# _ to space and dash after number
tdate[, "covid_phenotype"] <- gsub("_", " ", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("01 ", "01-", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("02 ", "02-", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("03 ", "03-", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("04 ", "04-", tdate[, "covid_phenotype"])

# rename specific phenotypes
tdate[, "covid_phenotype"] <- gsub("04-Fatal with covid diagnosis", "04-Fatal with diagnosis", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("04-Fatal without covid diagnosis", "04-Fatal no diagnosis", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("04-Covid inpatient death", "04-Inpatient death", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("Covid", "COVID-19", tdate[, "covid_phenotype"])
tdate[, "covid_phenotype"] <- gsub("covid", "COVID-19", tdate[, "covid_phenotype"])

# Correct order
unique(tdate[, "covid_phenotype"])
tdate[,"covid_phenotype"] <- factor(tdate[,"covid_phenotype"], levels = c("01-COVID-19 positive test", "01-GP COVID-19 diagnosis",
                                                                          "02-COVID-19 admission", "03-ICU admission",
                                                                          "03-NIV treatment", "03-IMV treatment",
                                                                          "03-ECMO treatment", "04-Fatal with diagnosis",
                                                                          "04-Fatal no diagnosis", "04-Inpatient death"))


# Create severity by aggregating 01/02/03/04
tdate[grepl("01",tdate[,"covid_phenotype"]),"severity"] <- "01 - COVID-19 positive / diagnosis"
tdate[grepl("02",tdate[,"covid_phenotype"]),"severity"] <- "02 - Hospitalisations"
tdate[grepl("03",tdate[,"covid_phenotype"]),"severity"] <- "03 - Ventilatory support"
tdate[grepl("04",tdate[,"covid_phenotype"]),"severity"] <- "04 - Fatal COVID-19"

tdate[,"severity"] <- factor(tdate[,"severity"], 
                             levels = c("01 - COVID-19 positive / diagnosis", "02 - Hospitalisations", "03 - Ventilatory support", "04 - Fatal COVID-19"))

tdate[,"covid_phenotype"] <- str_wrap(tdate[,"covid_phenotype"])


# Plot 1: Test Intervals --------------------------------------------------

# 'Supplementary Figure 2: Maximum number of days between consecutive positive COVID-19 tests'

p1 <- ggplot(tdate, aes(x = date, fill = covid_phenotype)) + geom_density(aes(y = ..count..), alpha = 0.25) + xlab("") + ylab("") + 
  facet_wrap(~severity, scales = "free", ncol = 1) + theme_bw() +   
  scale_x_date(date_breaks = "2 month", date_labels = "%b %Y") + scale_fill_manual(values = mycolors$color) + 
  theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust = 0.5), legend.key.height = unit(0.4,"cm"), 
        legend.key.width = unit(0.4, "cm"), legend.text = element_text(size = 7), legend.margin = margin(0,0,0,0),
        legend.box.margin = margin(0,0,0,0)) + guides(fill = guide_legend(title = "COVID-19 events          "))
p1


# Plot 2: Phenotype Timeline Hist (Group by severity) ---------------------
options(scipen = 999)
p2 <- ggplot(tdate, aes(x = date, fill = covid_phenotype)) + geom_histogram(position = "stack", bins = 100, color = "black") + xlab("") + ylab("") + 
  facet_wrap(~severity, scales = "free", ncol = 1) + theme_bw() +   
  scale_x_date(date_breaks = "2 month", date_labels = "%b %Y") + scale_fill_manual(values = mycolors$color) + 
  theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust = 0.5), legend.key.height = unit(0.4,"cm"), 
        legend.key.width = unit(0.4, "cm"), legend.text = element_text(size = 7), legend.margin = margin(0,0,0,0),
        legend.box.margin = margin(0,0,0,0)) + guides(fill = guide_legend(title = "COVID-19 events          "))
p2


# Plot 3: Phenotype Timeline Histogram ------------------------------------

# 'Figure 3: Timeline of COVID events' in our manuscript

tdate3 <- tdate
tdate3[, "covid_phenotype"] <- gsub("01-", "", tdate3[, "covid_phenotype"])
tdate3[, "covid_phenotype"] <- gsub("02-", "", tdate3[, "covid_phenotype"])
tdate3[, "covid_phenotype"] <- gsub("03-", "", tdate3[, "covid_phenotype"])
tdate3[, "covid_phenotype"] <- gsub("04-", "", tdate3[, "covid_phenotype"])
tdate3[, "covid_phenotype"] <- gsub("GP COVID-19 diagnosis", "GP diagnosis", tdate3[, "covid_phenotype"])
tdate3[, "covid_phenotype"] <- gsub("COVID-19 positive test", "Positive test", tdate3[, "covid_phenotype"])
tdate3[, "covid_phenotype"] <- gsub("COVID-19 admission", "Hospital admission", tdate3[, "covid_phenotype"])

# Chris rename again during releveling
unique(tdate3[, "covid_phenotype"])
tdate3[,"covid_phenotype"] <- factor(tdate3[,"covid_phenotype"], levels = c("GP diagnosis", "Positive test",
                                                                          "Hospital admission", "ICU admission",
                                                                          "NIV treatment", "IMV treatment",
                                                                          "ECMO treatment", "Fatal with diagnosis",
                                                                          "Fatal no diagnosis", "Inpatient death"))


p3 <- ggplot(tdate3, aes(x = date, fill = covid_phenotype)) + geom_density(aes(y = ..count..), alpha = 0.25) + xlab("") + ylab("") + 
  facet_grid(rows = vars(covid_phenotype), scales = "free") + scale_fill_manual(values = mycolors$color) + 
  theme_bw() +  scale_x_date(date_breaks = "2 month", date_labels = "%b %Y") + 
  theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust = 0.5), 
        strip.text = element_text(size = 5), legend.position = "None") +
  # Lockdown and wave definitions
  geom_vline(xintercept=as.Date("2020-03-20"), linetype="dashed", color="blue", size=0.3) +
  geom_vline(xintercept=as.Date("2020-03-26"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2020-05-29"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-09-30"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-10-31"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-01-06"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-02-12"), linetype="dashed", color="blue", size=0.3)  
p3

# Plot 4: Source Timeline Histograms --------------------------------------

# 'Supplementary Figure 1: Timeline plots of COVID-19 events by data source'

sdate <- unique(traject[,c("person_id_deid", "source", "date")])

p4 <- ggplot(sdate, aes(x = date, fill = source)) + geom_density(aes(y = ..count..), alpha = 0.25) + xlab("") + ylab("") + 
  facet_grid(rows = vars(source), scales = "free") +  theme_bw() +  scale_x_date(date_breaks = "2 month", date_labels = "%b %Y") + 
  theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust = 0.5), 
        strip.text = element_text(size = 6), legend.position = "None")+
  # Lockdown and wave definitions
  geom_vline(xintercept=as.Date("2020-03-20"), linetype="dashed", color="blue", size=0.3) +
  geom_vline(xintercept=as.Date("2020-03-26"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2020-05-29"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-09-30"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-10-31"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-01-06"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-02-12"), linetype="dashed", color="blue", size=0.3)  

p4

head(traject)
source("~/dars_nic_391419_j3w9t_collab/CCU013/CCU013_colorscheme.R")
trajdata <- dbGetQuery(con, "SELECT DISTINCT person_id_deid, trajectory_phenotype, date FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_graph_data")
trajdata$trajectory_phenotype <- factor(trajdata$trajectory_phenotype, levels = c("Positive test", "GP Diagnosis", "Hospitalisation", "Critical care", "Death"))
p5 <- ggplot(trajdata, aes(x = date, fill = trajectory_phenotype)) + geom_density(aes(y = ..count..), alpha = 0.25) + xlab("") + ylab("") + 
  theme_bw() +  scale_x_date(date_breaks = "2 month", date_labels = "%b %Y") + 
  scale_fill_manual(values = mycolors2$color) + 
  theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust = 0.5), strip.text = element_text(size = 6), legend.position = "top")+
  # Lockdown and wave definitions
  geom_vline(xintercept=as.Date("2020-03-20"), linetype="dashed", color="blue", size=0.3) +
  geom_vline(xintercept=as.Date("2020-03-26"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2020-05-29"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-09-30"), linetype="dashed", color="blue", size=0.3) + 
  geom_vline(xintercept=as.Date("2020-10-31"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-01-06"), linetype="dashed", color="red", size=0.2) + 
  geom_vline(xintercept=as.Date("2021-02-12"), linetype="dashed", color="blue", size=0.3)  

p5



# Export ------------------------------------------------------------------


pdf("~/dars_nic_391419_j3w9t_collab/CCU013/output/CCU013_timeline_covid_pheno_density.pdf", height = 9, width = 7)
p1
dev.off()

pdf("~/dars_nic_391419_j3w9t_collab/CCU013/output/CCU013_timeline_covid_pheno_hist.pdf", height = 9, width = 7)
p2
dev.off()

pdf("~/dars_nic_391419_j3w9t_collab/CCU013/output/CCU013_timeline_covid_pheno_density_not_grouped.pdf", height = 9, width = 7)
p3
dev.off()

pdf("~/dars_nic_391419_j3w9t_collab/CCU013/output/CCU013_timeline_covid_source_density_not_grouped.pdf", height = 9, width = 7)
p4
dev.off()

pdf("~/dars_nic_391419_j3w9t_collab/CCU013/output/CCU013_timeline_all_events_trajectory_companion.pdf", height = 4.5, width = 8)
p5
dev.off()

# Misc --------------------------------------------------------------------




#legend.key.height = unit(0.4,"cm"), 
#legend.key.width = unit(0.4, "cm"), legend.text = element_text(size = 7), legend.margin = margin(0,0,0,0),
#legend.box.margin = margin(0,0,0,0)) + guides(fill = guide_legend(title = "Covid phenotype          "))


#ggplot(sum_date, aes(x = date, y = covid_phenotype, height = scales::rescale(Freq), group = covid_phenotype, fill = Freq)) + geom_ridgeline_gradient() + 
#  xlab("") + ylab("") + scale_x_date(date_breaks = "1 month", date_labels = "%b %Y") + 
#  theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust = 1)) + facet_wrap(~severity, scales = "free")
#geom_rid

