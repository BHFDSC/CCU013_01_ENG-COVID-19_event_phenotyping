d <- dbGetQuery(con, "SELECT person_id_deid, date, covid_phenotype, source FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort")

uniphenos <- sort(unique(d$covid_phenotype))
myrows <- c("Min._date", "Max._date", uniphenos)
mysources <- unique(sort(d[,"source"]))

mytab <- data.frame(vars = myrows)

for(i in 1:length(mysources)){
  mytab[which(mytab[,1]=="Min._date"), mysources[i]] <- as.character(min(d[which(d$source == mysources[i]), "date"]))
  mytab[which(mytab[,1]=="Max._date"), mysources[i]] <- as.character(max(d[which(d$source == mysources[i]), "date"]))
  for(x in 1:length(uniphenos)){
    mytab[which(mytab[,1]==uniphenos[x]), mysources[i]] <- 
      as.character(length(unique(d[which(d$source == mysources[i] & d$covid_phenotype == uniphenos[x]), "person_id_deid"])))
  }
  print(length(mysources) - i)
}
mytab

for(x in 1:length(uniphenos)){
  mytab[which(mytab[,1]==uniphenos[x]), "Total"] <- 
    as.character(length(unique(d[which(d$covid_phenotype == uniphenos[x]), "person_id_deid"])))
}

mytab
mytab[mytab=="0"] <- ""

write.table(mytab, "~/dars_nic_391419_j3w9t_collab/CCU013/output/tables/CCU013_supp_tab1_ids_by_source.txt", row.names = F, sep = "\t", quote = F) 
