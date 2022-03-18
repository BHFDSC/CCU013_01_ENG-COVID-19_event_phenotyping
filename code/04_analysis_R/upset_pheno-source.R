library(dplyr)
library(UpSetR)
library(RColorBrewer)
library(grDevices)
library(testthat)
library(grid)

warning("Requires authenticated connection to Spark saved as 'con'. \n
        Will attempt to load but if running as another user need to configure connection manually.")
source("/mnt/efs/c.tomlinson/dbConnect.R")

warning("Must not have {ComplexUpset} loaded")
detach("package:ComplexUpset", unload=TRUE)


# Functions ---------------------------------------------------------------

# Use reverse due to the way upset is ordering by frequency

barPal = function(listIn, brewerPal="YlOrRd"){
  # Constrain to the visible end of brewer palette
  # Interpolate between for the required number of colours based on Upset Input List
  colorRampPalette(rev(brewer.pal(9, brewerPal))[1:5])(2^length(listIn)-1)
}

sourcePal = function(listIn){
  # Custom dictionary 
  # Add extra Death phenotypes same colour as deaths dataset
  # "Deaths (Dx)"    "Deaths (no Dx)"
  pal_names = c("SGSS", "GDPPR", "HES APC", "SUS", "CHESS", "HES CC", "Deaths", "Deaths (Dx)", "Deaths (no Dx)")
  pal_sources = rev(c("#1B9E77", 
                       "#1B9E77", "#1B9E77", 
                      "#D95F02", "#7570B3", "#E7298A", "#66A61E", "#E6AB02", "#666666"))
  pal_sources = data.frame("Dataset" = pal_names, "Colour" = pal_sources)
  # Get relevant values
  names(listIn) %>% 
    data.frame("Dataset" = .) %>% 
    # Left join to keep order
    left_join(pal_sources, by='Dataset') -> pal
  return(pal[['Colour']])
}


setPal = function(listIn, brewerPal="BuPu"){
  # brewer.pal() can't handle <3 colours
  if (length(listIn) < 3){
    pal = brewer.pal(3, brewerPal)[1:length(listIn)] %>% rev()
    pal = colorRampPalette(rev(brewer.pal(9, brewerPal))[1:5])(length(listIn))
  } else {
    pal =  brewer.pal(length(listIn), brewerPal) %>% rev()
  }
  # Overwrite the above using just the visible end of brewer pals
  pal = colorRampPalette(rev(brewer.pal(9, brewerPal))[1:5])(length(listIn))
  return(pal)
}

# Test palette functions
# expect_true(barPal(listECMO) %>% length() == 2^length(listECMO)-1)
# expect_true(barPal(listICU) %>% length() == 2^length(listICU)-1)
# expect_true(setPal(listECMO) %>% length() == length(listECMO))
# expect_true(setPal(listICU) %>% length() == length(listICU))

# Create plot function
upsetPlot = function(listIn){
  plot = upset(fromList(listIn), 
               order.by = "freq",
               empty.intersections = "on",
               # Specify set names to match with palette
               sets = names(listIn),
               keep.order = TRUE,
               main.bar.color = barPal(listIn),
               mainbar.y.label = "Intersection (individuals)",
               sets.bar.color = sourcePal(listIn),
               sets.x.label = "Cohort (individuals)",
               matrix.color="black",
               matrix.dot.alpha = 0.5 # transparency of empty intersections
               )
  return(plot)
}


# Import data -------------------------------------------------------------


# Get distinct combination of events & sources
events_sources = dbGetQuery(con, 
                            "SELECT distinct covid_phenotype, source 
                            FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort")

# Iterate through combinations and extract ids
# Assign extracted ids to a dataframe of the name of that combination of event_source
# e.g. {pheno}_{dataset}
for (i in 1:nrow(events_sources)){
  event = events_sources$covid_phenotype[i]
  source = events_sources$source[i]
  # Use distinct to reduce vector size
  sql = paste0("SELECT distinct person_id_deid as id 
               FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort 
               WHERE ",
               "covid_phenotype = \'", event, "\'", 
               " AND source = \'", source, "\'")
  assign(paste0(event, "_", source),
         dbGetQuery(con, sql)
  )
}



# Prep data ---------------------------------------------------------------

# Will use UpSetR::fromList() - this is within plotting function
# Note must convert dataframes to vector with unlist() first
# This has the benefit of making operations much faster

list01 = list("SGSS" = `01_Covid_positive_test_SGSS` %>% unlist(),
              "GDPPR" = `01_GP_covid_diagnosis_GDPPR` %>% unlist()
              )

listAdmission = list("HES APC" = `02_Covid_admission_HES APC` %>% unlist(),
                  "SUS" = `02_Covid_admission_SUS` %>% unlist(),
                  "CHESS" = `02_Covid_admission_CHESS` %>% unlist()
                  )

listICU = list("HES CC" = `03_ICU_admission_HES CC` %>% unlist(),
               "CHESS" = `03_ICU_admission_CHESS` %>% unlist()
               )

listNIV = list("HES APC" = `03_NIV_treatment_HES APC` %>% unlist(),
               "SUS" = `03_NIV_treatment_SUS` %>% unlist(),
               "HES CC" = `03_NIV_treatment_HES CC` %>% unlist(),
               "CHESS" = `03_NIV_treatment_CHESS` %>% unlist()
               )

listIMV = list("HES APC" = `03_IMV_treatment_HES APC` %>% unlist(),
               "SUS" = `03_IMV_treatment_SUS` %>% unlist(),
               "HES CC" = `03_IMV_treatment_HES CC` %>% unlist(),
               "CHESS" = `03_IMV_treatment_CHESS` %>% unlist()
               )

listECMO = list("HES APC" = `03_ECMO_treatment_HES APC` %>% unlist(),
                "SUS" = `03_ECMO_treatment_SUS` %>% unlist(),
                "CHESS" = `03_ECMO_treatment_CHESS` %>% unlist()
                )

listDeaths = list("HES APC" = `04_Covid_inpatient_death_HES APC` %>% unlist(),
                  "SUS" = `04_Covid_inpatient_death_SUS` %>% unlist(),
                  "Deaths (Dx)" = `04_Fatal_with_covid_diagnosis_deaths` %>% unlist(),
                  "Deaths (no Dx)" = `04_Fatal_without_covid_diagnosis_deaths` %>% unlist()
                  )



# Plot! -------------------------------------------------------------------


# upsetPlot(list01)


# Plots not formatting correctly when using png(), 
# therefore just manually exported from plot viewer given time!

# png("~/dars_nic_391419_j3w9t_collab/CCU013/upset/upset_admissions.png", 
#     height = 1200, 
#     width = 1200, 
#     res = 300)
# dev.off()

# png("~/dars_nic_391419_j3w9t_collab/CCU013/upset/upset_admissions.png", 
#         height = 900,
#         width = 1200,
#         res = 300)
# GG save is too zoomed in only on set bit
# ggsave("~/dars_nic_391419_j3w9t_collab/CCU013/upset/upset_admissions.png",
#        height = 900,
#        width = 1200,
#        dpi=300)


upsetPlot(listAdmission)
grid.text("Admissions", 
          x = 0.65, y = 0.95,
          gp = gpar(fontsize = 12))


upsetPlot(listICU)
grid.text("ICU", 
          x = 0.65, y = 0.95,
          gp = gpar(fontsize = 12))


upsetPlot(listNIV)
grid.text("NIV", 
          x = 0.65, y = 0.95,
          gp = gpar(fontsize = 12))

upsetPlot(listIMV)
grid.text("IMV", 
          x = 0.65, y = 0.95,
          gp = gpar(fontsize = 12))

upsetPlot(listECMO)
grid.text("ECMO", 
          x = 0.65, y = 0.95,
          gp = gpar(fontsize = 12))


upsetPlot(listDeaths)
grid.text("Deaths", 
          x = 0.65, y = 0.95,
          gp = gpar(fontsize = 12))




