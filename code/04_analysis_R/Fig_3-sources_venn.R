# R script to create main Venn diagram figure for the COVID-19 severity phenotyping paper
# Author: Johan Hilge Thygesen
# Last modified 16.06.21


#library("nVennR")
library("VennDiagram")

traject <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_cohort_paper")
length(unique(traject$person_id_deid))
dsource <- unique(traject[,c("person_id_deid", "source")])

#unique(dsource[,"source"])

### VennDiagram code
area1 <- unique(dsource[which(dsource[,"source"] %in% c("GDPPR")), "person_id_deid"])             # Primary Care
area2 <- unique(dsource[which(dsource[,"source"] %in% c("HES APC", "HES CC", "CHESS", "SUS")), "person_id_deid"]) # Hospital episodes
area3 <- unique(dsource[which(dsource[,"source"] %in% c("SGSS")), "person_id_deid"])  # Laboratory Tests
area4 <- unique(dsource[which(dsource[,"source"] %in% c("deaths")), "person_id_deid"])            # Deaths

n12 <- unique(area1[which(area1 %in% area2)])
n13 <- unique(area1[which(area1 %in% area3)])
n14 <- unique(area1[which(area1 %in% area4)])
n23 <- unique(area2[which(area2 %in% area3)])
n24 <- unique(area2[which(area2 %in% area4)])
n34 <- unique(area3[which(area3 %in% area4)])
n123 <- unique(n12[which(n12 %in% area3)])
n124 <- unique(n12[which(n12 %in% area4)])
n134 <- unique(n13[which(n13 %in% area4)])
n234 <- unique(n23[which(n23 %in% area4)])
n1234 <- unique(n123[which(n123 %in% area4)])
all <- unique(traject[,"person_id_deid"])

output <- data.frame(group = c("area1", "area2", "area3", "area4", "n12", "n13", "n14", "n23", "n24", "n34", "n123", "n124", "n134", "n234", "n1234", "all"),
                     size = c(length(area1), length(area2), length(area3), length(area4), 
                              length(n12), length(n13), length(n14), length(n23), length(n24), 
                              length(n34), length(n123), length(n124), length(n134), length(n234), length(n1234), length(all)))

head(output)

output[,"percent"] <- round(output[,"size"]/length(unique(traject[,1])),digits = 4)
sum(output[,"percent"])
write.table(output, "~/dars_nic_391419_j3w9t_collab/CCU013/output/CCU013_venn_numbers.txt", sep = "\t", quote = F, row.names = F)

mycolors = c('#74C476', '#FE9929', '#B3CDE3', '#3182BD')
mycats = c(paste0("Primary Care\n(n=",length(area1), ")"), 
           paste0("Secondary \nCare\n(n=",length(area2), ")"),
           paste0("COVID-19 Testing\n(n=",length(area3), ")"),
           paste0("Deaths\n(n=",length(area4), ")"))

pdf("~/dars_nic_391419_j3w9t_collab/CCU013/output/CCU013_venn.pdf")
draw.quad.venn(length(area1), length(area2), length(area3), length(area4), n12 = length(n12), 
                    n13 = length(n13), n14 = length(n14), n23 = length(n23), n24 = length(n24), n34 = length(n34), 
                    n123 = length(n123), n124 = length(n124), n134 = length(n134), n234 = length(n234), 
                    n1234 = length(n1234), col = mycolors, fill = mycolors, label.cex = 3, category = mycats)
dev.off()
output

#########################
### nVennR code
primary_care <- subset(dsource, source == "GDPPR")$person_id_deid
labtests <- subset(dsource, source %in% c("SGSS", "pillar_2"))$person_id_deid
hospital <- subset(dsource, source %in% c("HES APC", "hes_cc"))$person_id_deid
deaths <- subset(dsource, source %in% c("deaths"))$person_id_deid
myV <- plotVenn(list(Primary_care = primary_care, Labtests = labtests, Hospitalised = hospital, Deaths = deaths), nCycles = 2000)

png("Test.png")
myV
dev.off()
showSVG(nVennObj = myV, outFile = "VennDiagram.svg")

