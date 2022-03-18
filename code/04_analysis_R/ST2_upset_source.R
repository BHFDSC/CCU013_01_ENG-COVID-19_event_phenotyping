library(dplyr)
library(UpSetR)
library(RColorBrewer)
library(grDevices)
library(grid)

warning("Requires authenticated connection to Spark saved as 'con'. \n
        Will attempt to load but if running as another user need to configure connection manually.")

source("/mnt/efs/c.tomlinson/dbConnect.R")


# Import data -------------------------------------------------------------


# Get distinct sources
sources = dbGetQuery(con, "SELECT distinct source FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort")

# Iterate through sources and extract ids
# Assign extracted ids to a corresponding dataframe 
# e.g. all_{source}

for (i in 1:nrow(sources)){
  source = sources$source[i]
  # Use distinct to reduce final IDs vector size
  sql = paste0("SELECT distinct person_id_deid as id 
               FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort 
               WHERE ",
               "source = \'", source, "\'")
  assign(paste0("all_", source),
         dbGetQuery(con, sql)
  )
}



# Prep data ---------------------------------------------------------------

# Will use UpSetR::fromList() - this is within plotting function
# Note must convert dataframes to vector with unlist() first
# This has the benefit of making operations much faster

listAll = list("SGSS" = all_SGSS %>% unlist(),
               "GDPPR" = all_GDPPR %>% unlist(),
               "HES APC" = `all_HES APC` %>% unlist(),
               "SUS" = all_SUS %>% unlist(),
               "HES CC" = `all_HES CC` %>% unlist(),
               "CHESS" = all_CHESS %>% unlist(),
               "Deaths" = all_deaths %>% unlist()
               )

# Plot! -------------------------------------------------------------------

# brewer.pal(8, "Dark2") without "#A6761D" 
pal_sources = rev(c("#1B9E77", "#D95F02", "#7570B3", "#E7298A", "#66A61E", "#E6AB02", "#666666"))


# options(scipen=999)
options(scipen=0, digits=7) # defaults

# Try to edit font-sizes via setting theme, does not work

# plot_theme = theme_void() %+replace%
#   theme(text = element_text(family = "Times", size=72),
#         axis.text=element_text(size=72), #change font size of axis text
#         axis.title=element_text(size=72), #change font size of axis titles
#         plot.title=element_text(size=72), #change font size of plot title
#         legend.text=element_text(size=72), #change font size of legend text
#         legend.title=element_text(size=72)
#         )
# plot_theme %>% theme_set()

upset(fromList(listAll), 
      # Manually order sets in ~'severity' as per Spiros
      sets = rev(c("SGSS", "GDPPR", "HES APC", "SUS", "CHESS", "HES CC", "Deaths")),
      keep.order = TRUE,
      # Show all sets
      nsets = 7,
      order.by = "freq",
      empty.intersections = "off", # Set as off else too many ? 127
      # scale.intersections = "log10",
      # scale.sets = "log10",
      main.bar.color = heat.colors(127),
      mainbar.y.label = "Intersection (individuals)",
      # rotate bar labels
      number.angles = 45, 
      # Consider manual palette for datasets
      sets.bar.color = pal_sources,
      sets.x.label = "Cohort (individuals)",
      matrix.color="black",
      # transparency of empty intersections
      matrix.dot.alpha = 0.8 
) 







