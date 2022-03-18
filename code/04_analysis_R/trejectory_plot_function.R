## Trajectory Plot function
graph_plotter <- function(g, mylayout, n_ids, size_column, width_column, days_column, plot_days, subtitle_on_plot, N_total_pop, 
                          threshold, n_unaffected){
  mysizes <- get.vertex.attribute(g, size_column)
  names(mysizes) <- names(V(g))
  
  options(scipen = 999)
  widths <- get.edge.attribute(g, width_column)
  mdays <- get.edge.attribute(g, days_column)
  
  threshold1 <- threshold
  threshold2 <- threshold * 10
  round_digts <- 1 # Nb this should be a factor 100
  
  if(plot_days == F){
    # Plot frequency of transitions if above threshold1
    E(g)$labels <- ifelse(widths / n_ids > threshold1, 
                          paste0(signif(widths / n_ids * 100, round_digts), '%'),
                          "")
  }else{
    # Plot median days between if frequency of transitions is above threshold1
    # E(g)$labels <- ifelse(widths / n_ids > threshold1, paste0(mdays), "") # Days only
    E(g)$labels <- ifelse(widths / n_ids > threshold1, paste0(signif(widths / n_ids * 100, round_digts), '[',mdays, ']'), "") # freq%[days]
  }
  
  # Defult line types
  # arrow_widths <- signif(widths / n_ids * 100, round_digts) * 10 # Dose not work great with very big differences between strate, i.e 5% vs 0.3%.
  # arrow_widths <- rep(widths, length(widths))
  arrow_widths <- 0.1
  arrow_colors <- rep("grey", length(widths))
  # make lines and labels over threshold1 darker gray
  arrow_colors[widths / n_ids > threshold1] <- "black"
  arrow_widths[widths / n_ids > threshold1] <- 1
  # make lines and arrows fat if above threshold2
  arrow_colors[widths / n_ids > threshold2] <- "black"
  # make lines black and fatter if above threshold3
  arrow_widths[widths / n_ids > threshold2] <- 3
  
  n_covid_event <- sum(mysizes[-which(names(mysizes)=="Unaffected")])
  resized <- (rescale(c(N_total_pop, mysizes, 0), to = c(0.5, 5)) * 20)
  #resized <- rescale(log(c(N_total_pop, mysizes, 0.1)), c(0.1,5))*2
  resized <- resized[2:(length(resized)-1)]
  
  plot(g,
       vertex.size = resized, # Make sizes relative to total population
       edge.width = arrow_widths,
       edge.arrow.size = 0.5,
       edge.color = arrow_colors,
       vertex.color = V(g)$color, 
       remove.multiple = F,
       edge.label.color= arrow_colors,
       edge.curved = 0.2,
       vertex.label.dist = -1,
       edge.label.cex = 0.9,
       edge.label = E(g)$labels,
       layout = mylayout
  )
  mtext(paste0("N individuals: ", format(n_ids, big.mark = ","),
               " - N affected: ", format(n_ids - n_unaffected, big.mark = ","),
               " - N transitions: ", format(sum(widths), big.mark = ",")), side = 3, padj = 2)
  mtext(subtitle_on_plot, side = 3) # this is plotted above the mtext above so its more like a title!
}

find_trajectories <- function(tdata){
  start_time <- Sys.time()
  uniid <- data.frame(person_id_deid = unique(tdata[,c("person_id_deid")]))
  print(paste0("Finding trajectories for ", nrow(uniid), " unique individuals..."))
  
  # Chunk control to minize-size of the trajecotry table to look up each ID in
  #   - The outer while loop reduces the size of the trajectory table to make loop up of ids faster 
  chunk_size <- 9999 # number of IDs in trajectory subset per outer loop!
  chunk_max <- nrow(uniid)
  chunk <- 1
  chunk_end <- chunk + chunk_size
  while(chunk<chunk_max){
    if(chunk > chunk_max){chunk <- chunk_max}
    chunk_end <-  ifelse(chunk_end > chunk_max, chunk_end <- chunk_max, chunk + chunk_size)
    print(paste0("Finding paths for IDs: ", chunk, "-", chunk_end))
    # create a subset of the trajectory table to lookup trajectories in for each ID
    tsub <- tdata[which(tdata[,1]%in%uniid[chunk:chunk_end, "person_id_deid"]), ]
    ## Finding trajectories; 1) finds all phenotypes per ID, 2) removes consecutive duplicate phenotypes, 3) collaps to path string, 4 count or add new path to trajectory table 
    uniid[chunk:chunk_end, "trajectory"] <- sapply(uniid[chunk:chunk_end, "person_id_deid"], function(X){paste(
      unique(tsub[which(tsub[,1]==X), "trajectory_phenotype"]), collapse = "->")})
    uniid[chunk:chunk_end, "days"] <- sapply(uniid[chunk:chunk_end, "person_id_deid"], function(X){paste(
      unique(tsub[which(tsub[,1]==X), "days_passed"]), collapse = "->")})
    chunk <- chunk + chunk_size + 1
  }
  # Remove empty rows added from the chunking if present!
  if(any(is.na(uniid[,1]))){
    print(nrow(uniid))
    uniid <- uniid[-which(is.na(uniid[,1])),] 
    print(nrow(uniid))
  }
  ## Tidy up the days between string by setting NA's proper and removing the first NA calculation
  uniid[which(uniid[,"days"]=="NA"),"days"] <- NA
  uniid[,"days"] <- sub("NA->", "", uniid[,"days"])
  if(any(grepl("NA->", uniid[,"days"]))){stop("Error NA-> detected in daysbetween string (days column!)")}
  print(paste0("Finished in: ", format(Sys.time() - start_time,2))) 
  return(uniid)
}


create_trajectory_plot <- function(uniid, tdata, folder_path, basefilename, prefix_file_name, subtitle_on_plot, graph_layout, plot_days = T, N_total_pop, 
                                   pre_processing_done, threshold, n_unaffected){
  start_time <- Sys.time()
  filename <-  paste0(folder_path, basefilename, prefix_file_name, ".txt")
  filename_paths <- paste0(folder_path, basefilename, "_pairwise-paths_", prefix_file_name, ".txt")
  filename_nodes <- paste0(folder_path, basefilename, "_node-sizes_", prefix_file_name, ".txt")
  
  # If pre_processing_done then skip all caluclations parts
  if(!pre_processing_done){
    
    #---- Create the trajectories table
    trajectories <- as.data.frame(table(uniid$trajectory))
    colnames(trajectories) <- c("trajectory", "count")
    trajectories$trajectory <- as.character(trajectories$trajectory)
    
    
    # --- Add counts of each trajectory phenotype ----------------------------------
    print("Adding counts for each phenotype...")
    covid_phenos <- unique(tdata$trajectory_phenotype)
    for(i in 1:length(covid_phenos)){
      uniid[which(uniid[,1]%in%tdata[which(tdata[,"trajectory_phenotype"]==covid_phenos[i]),"person_id_deid"]), covid_phenos[i]] <- 1
      trajectories[,covid_phenos[i]] <- sapply(trajectories[,"trajectory"], function(X){sum(uniid[,covid_phenos[i]] == 1 & uniid[,'trajectory']==X, na.rm = T)})
    }
    if(sum(trajectories[,"count"]) != length(unique(tdata[,"person_id_deid"]))){stop("Individuals are missing in the trajectory table!")}
    
    # Save output
    trajectories <- trajectories[order(trajectories[,"count"], decreasing = T), ]
    trajectories[,"percent"] <- round(trajectories[,"count"] / nrow(uniid) * 100, digits = 2)
    print("")
    print(paste0("Saving trajectories to: ", filename))
    write.table(trajectories, filename, row.names = F, quote =F , sep = "\t")  
    
    
    # ---- Create input for igraph plotting of graphs -------------------------
    # This step breaks up the trajectories into pairwise transitions and records all info for these for plotting
    print("")
    print("Create input for igraph plotting...")
    
    ## Identify first pair to initiate lists
    mymatch <- grep("->", trajectories[,"trajectory"])[1]
    xstrings <- unlist(strsplit(trajectories[mymatch,"trajectory"], "->")) # strings are phenotypes
    firstpair <- paste0(xstrings[1], "->", xstrings[2])
    
    ## Initiate pairwise path data frame
    pairwise_paths <- data.frame(pair = firstpair, 
                                 node1 = xstrings[1], 
                                 node2 = xstrings[2], 
                                 size = 0)
    
    # Initialize a list of path transitions to store number of days between events observed per individual
    daysbetween <- list()
    
    ## Calculate size of nodes
    size_of_nodes <- data.frame(phenotypes = unique(tdata[,"trajectory_phenotype"]), size = 0)
    
    # Fill pairwise path, size of nodes and days-between for all trajectories.
    for(i in 1:nrow(trajectories)){
      xstrings <- unlist(strsplit(trajectories[i,"trajectory"], "->")) # strings are phenotypes
      
      if(length(xstrings)>1){
        ## Get days between events for all individuals and store in the daysbetween list
        ## (e.g for 3 strings the two columns represents the days between event a - b, b - c)
        xvalues <- uniid[which(uniid[,"trajectory"] == trajectories[i,1]), "days"]
        for(a in 1:(length(xstrings)-1)){
          pathname <- paste0(xstrings[a], "->", xstrings[a+1])
          # Append path transition to list if not in already 
          if(!pathname %in% names(daysbetween)){ daysbetween[[pathname]] <- c(0)} # NB! This does not work if the initial vector is empty c(), I subtract the initial zero later on!
          newnumbers <- as.numeric(sapply(strsplit(xvalues, "->"), "[", a))
          newnumbers[is.na(newnumbers)] <- 0 
          daysbetween[[pathname]] <- c(newnumbers, as.numeric(unlist(daysbetween[pathname])))
        }
        
        matched_path <- which(size_of_nodes[,"phenotypes"] == xstrings[1])
        size_of_nodes[matched_path, "size"] <- size_of_nodes[matched_path, "size"] + trajectories[i,"count"]
        for(j in 2:length(xstrings)){
          matched_path <- which(size_of_nodes[,"phenotypes"] == xstrings[j])
          size_of_nodes[matched_path, "size"] <- size_of_nodes[matched_path, "size"] + trajectories[i,"count"]
          
          pair <- paste0(xstrings[j-1], "->", xstrings[j])
          if(pair %in% pairwise_paths[,"pair"]){
            matched_dpath <- which(pairwise_paths[,"pair"]==pair)
            pairwise_paths[matched_dpath, "size"] <- pairwise_paths[matched_dpath, "size"] + trajectories[i,"count"]
            
          }else{
            newrow <- nrow(pairwise_paths) + 1
            pairwise_paths[newrow, "pair"] <- pair
            pairwise_paths[newrow, "node1"] <- xstrings[j-1]
            pairwise_paths[newrow, "node2"] <- xstrings[j]
            pairwise_paths[newrow, "size"] <- trajectories[i,"count"]
          }
        }
      }else{
        matched_path <- which(size_of_nodes[,"phenotypes"] == xstrings)
        size_of_nodes[matched_path, "size"] <- size_of_nodes[matched_path, "size"] + trajectories[i,"count"]
      }
    }
    
    # Add phenotype counts
    for(i in 1:length(covid_phenos)){
      pairwise_paths[which(pairwise_paths$node1 == covid_phenos[i] | pairwise_paths$node2 == covid_phenos[i]), covid_phenos[i]] <-
        pairwise_paths[which(pairwise_paths$node1 == covid_phenos[i] | pairwise_paths$node2 == covid_phenos[i]),"size"]
    }
    
    # Add days between
    for(i in 1:nrow(pairwise_paths)){
      pathname <- pairwise_paths[i,1]
      # Last number should always be 0 as that is how the list of days between numbers got initiated
      all_day_measures <- daysbetween[[pathname]]
      if(tail(all_day_measures, 1) != 0){stop("Something has gone wrong with the days between calculations - last number not 0!")}
      all_day_measures <- all_day_measures[1:(length(all_day_measures)-1)] # Remove that inital 0 which was used to start the list!
      pairwise_paths[i,"n_day_measures"] <- length(daysbetween[[pathname]])
      pairwise_paths[i,"median_days"] <-  median(daysbetween[[pathname]])
      pairwise_paths[i,"mean_days"] <- mean(daysbetween[[pathname]]) 
    }
    
    ## Write out paths and node sizes
    print("")
    print(paste0("Pair wise paths writen to: ", filename_paths))
    print(paste0("Sizes of nodes writen to: ", filename_nodes))
    write.table(pairwise_paths, filename_paths, sep = "\t", quote = F, row.names = F)
    write.table(size_of_nodes, filename_nodes, sep = "\t", quote = F, row.names = F)
    
  }else{
    pairwise_paths <- read.table(filename_paths, header = T, sep = "\t")
    size_of_nodes <- read.table(filename_nodes, header = T, sep = "\t")
  }
  
  # --------- Plotting ---------------------------------------------------------
  print("")
  print("Generating plots...")
  net <- graph_from_data_frame(d = pairwise_paths[,c("node1", "node2")], 
                               vertices = unique(c(pairwise_paths[,"node1"], pairwise_paths[,"node2"])),
                               directed = TRUE)
  
  # Assign variables to graph
  V(net)$color <- mycolors[order(match(mycolors3$covid_phenotype, V(net)$name)),'color']
  V(net)$size <- size_of_nodes[order(match(size_of_nodes$phenotypes, V(net)$name)), "size"]
  E(net)$width <- pairwise_paths$size
  E(net)$median_days <- pairwise_paths$median_days
  E(net)$mean_days <- pairwise_paths$mean_days
  
  ## Fix median days from unaffected for wave 1 and 2
  ## By subtracting the number of days from study start to the start of wave 1 or 2
  ##   Wave 1: Days from 2020-01-23 to 2020-03-20 = 57
  ##   Wave 2: Days from 2020-01-23 to 2020-09-30 = 251
  if(grepl("wave1", prefix_file_name)){
    # edgelist(net)[,1] gives the starting point of each edge
    E(net)$median_days[as_edgelist(net)[,1]=="Unaffected"] <- E(net)$median_days[as_edgelist(net)[,1]=="Unaffected"] - 57
  }
  if(grepl("wave2", prefix_file_name)){
    E(net)$median_days[as_edgelist(net)[,1]=="Unaffected"] <- E(net)$median_days[as_edgelist(net)[,1]=="Unaffected"] - 251
  }
  
  ## Fix layout
  l <- suppressWarnings(layout.reingold.tilford(net))
  graph_layout <- graph_layout[order(match(graph_layout$phenotype, names(V(net)))),]
  my_layout <- matrix(c(graph_layout$x, graph_layout$y), ncol = 2, byrow = F)
  
  print("")
  filename_plot <- paste0(folder_path, '/', basefilename, "_", prefix_file_name, ".pdf")
  print(paste0("Pairwise paths writen to: ", filename_plot))
  
  ## plot figure
  pdf(filename_plot)
  graph_plotter(net, 
                mylayout = my_layout, 
                n_ids = nrow(uniid), 
                size_column = "size", 
                width_column = "width", 
                days_column = "median_days", 
                plot_days, subtitle_on_plot, N_total_pop, threshold,
                n_unaffected)
  dev.off()
  print(paste0("Finished in: ", format(Sys.time() - start_time,2))) 
}
