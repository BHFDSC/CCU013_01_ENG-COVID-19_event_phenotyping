library("data.table")

## Wave 1
d <- as.data.frame(fread("~/dars_nic_391419_j3w9t_collab/CCU013/output/trajectories/CCU013_trajectories_ICU_individual_trajectories_wave1.txt"))
head(d)

nrow(d[which(d$trajectory=="Unaffected->Positive test"),]) # 58413
nrow(d[which(d$trajectory=="Unaffected->Primary care diagnosis"),]) # 32051
nrow(d[which(d$trajectory=="Unaffected->Hospitalisation"),]) # 13513
nrow(d[which(d$trajectory=="Unaffected->ICU admission"),]) # 0
nrow(d[which(d$trajectory=="Unaffected->Death"),]) # 7869

## Wave 1
d <- as.data.frame(fread("~/dars_nic_391419_j3w9t_collab/CCU013/output/trajectories/CCU013_trajectories_ICU_individual_trajectories_wave2.txt"))

nrow(d[which(d$trajectory=="Unaffected->Positive test"),]) # 785065
nrow(d[which(d$trajectory=="Unaffected->Primary care diagnosis"),]) # 182166
nrow(d[which(d$trajectory=="Unaffected->Hospitalisation"),]) # 6696
nrow(d[which(d$trajectory=="Unaffected->ICU admission"),]) # 0
nrow(d[which(d$trajectory=="Unaffected->Death"),]) # 1972
