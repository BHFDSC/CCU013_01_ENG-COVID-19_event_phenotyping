library("data.table")

# Paths
folder_path <- "~/dars_nic_391419_j3w9t_collab/CCU013/output/trajectories/"
basefilename <- "CCU013_trajectories_ICU"

prefixs <- c("wave1", "wave2")
phenotypes <- c("Positive test", "Primary care diagnosis", "Hospitalisation", "ICU admission", "Death")

for(i in 1:length(prefixs)){
  nodesize <- read.table(paste0(folder_path, basefilename, "_node-sizes_", prefixs[i], ".txt"), header = T, sep = "\t")
  pairwise <- read.table(paste0(folder_path, basefilename, "_pairwise-paths_", prefixs[i], ".txt"), header = T, sep = "\t")
  trajectories <- as.data.frame(fread(paste0(folder_path, basefilename, "_individual_trajectories_", prefixs[i], ".txt"), header = T, sep = "\t"))
 

  ## Update node sizes to include only counts
  for(x in 1:length(phenotypes)){
    n <- nrow(trajectories[which(trajectories$trajectory == paste0("Unaffected->", phenotypes[x])),])
    n_all <- nodesize[which(nodesize[,"phenotypes"] == phenotypes[x]),"size"]
    print(paste0("Only ", phenotypes[x], ": ",n, " of ", n_all))
  }
}

