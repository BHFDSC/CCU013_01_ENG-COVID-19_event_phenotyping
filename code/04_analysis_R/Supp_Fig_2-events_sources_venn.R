---
title: 'Supplementary Figure 2: Venn Diagram of COVID-19 events and constituent data sources'
author: 'Chris Tomlinson'
date: 'Knitted on `r date()`'

output:
  html_document:
    df_print: paged
---


**Description** 

This notebook extracts the unique `person_id_deids` for each combination of `covid_phenotype` (`event`) and `source` from `ccu013_covid_trajectory`. For each `event` it then produces a vennDiagram of `sources`.  
  
> Venn Diagrams illustrating the numbers of individuals experiencing each COVID-19 event. Positive tests, Primary care diagnoses and deaths with COVID-19 diagnosis, or within 28 days of a positive test, are not shown as these are derived from a single data source.

  
**Project(s)** CCU013

**Paper** Characterising COVID-19 related events in a nationwide electronic health record cohort of 55.9 million people in England
 
**Author(s)** Chris Tomlinson
 
**Reviewer(s)** 
 
**Date last updated** 2021-06-29
 
**Date last reviewed** UNREVIEWED
 
**Date last run** `r date()`
 
**Data input**  
* `ccu013_covid_trajectory_cohort_paper`

**Data output**  
* Self-contained as the knitted `.html` file from this notebook  
* `.Rmd` for sharing on `GitHub`  

**Software and versions** `SQL`, `R`
 
**Packages and versions**

```{r config}
library(dplyr)
library(VennDiagram)
library(grDevices)
library(RColorBrewer)
library(gridExtra)

warning("Requires authenticated connection to Spark saved as 'con'. \n
        Will attempt to load but if running as another user need to configure connection manually.")
source("/mnt/efs/c.tomlinson/dbConnect.R")
```
```{r sessionInfo}
sessionInfo()
```

# Import Data  
```{r getdata}
# Get distinct combination of events & sources
events_sources = dbGetQuery(con, 
                            "SELECT distinct covid_phenotype, source 
                            FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort")

# Iterate through combinations and extract ids
# Assign extracted ids to a dataframe of the name of that combination of event_source
for (i in 1:nrow(events_sources)){
  event = events_sources$covid_phenotype[i]
  source = events_sources$source[i]
  sql = paste0("SELECT person_id_deid as id 
               FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory_paper_cohort 
               WHERE ",
              "covid_phenotype = \'", event, "\'", 
              " AND source = \'", source, "\'")
  assign(paste0(event, "_", source),
         dbGetQuery(con, sql)
  )
}
```
## Configure plots
```{r plotsetup}
# Helper function to display Venn diagram
display_venn <- function(x, ...){
  grid.newpage()
  venn_object <- venn.diagram(x, 
                              filename = NULL, 
                              # Specify defaults
                              alpha=0.3,
                              print.mode=c("raw", "percent"),
                              ...)
  grid.draw(venn_object)
}

# Mimic default ggplot colour behaviour
gg_color_hue <- function(n) {
  hues = seq(15, 375, length = n + 1)
  hcl(h = hues, l = 65, c = 100)[1:n]
}

# Build palette to match Johan's Supp Fig 1 of source timelines
pal_sources = data.frame(data_source = c("CHESS",
                                         "Deaths",
                                         "GDPPR",
                                         "HES APC",
                                         "HES CC",
                                         # Comment out as not included in final
                                         # "HES OP",
                                         # "Pillar 2",
                                         "SGSS",
                                         "SUS"),
                         color = gg_color_hue(7) # Amend to 9 if above included
                         ) %>% 
  # Make sure arranged alphabetically as this is what VennDiagram expects
  arrange(data_source)
  
pal_sources
```

# `01_...`

```{r all1}
display_venn(
  list(`01_GP_covid_diagnosis_GDPPR`$id,
       `01_Covid_positive_test_SGSS`$id),
  category.names = c("GDPPR" , "SGSS"),
  fill = #c("#A58AFF", "#53B400")
    # Becomes a nightmare 
    pal_sources[pal_sources$data_source == "GDPPR" | pal_sources$data_source == "SGSS",]$color
)
```

### `01_Covid_positive_test` i.e. tests
Currently not using Pillar 2 therefore code commented out

```{r 01_Covid_positive_test}
# Not using pillar 2 therefore just one source
# display_venn(
#   list(`01_Covid_positive_test_SGSS`$id,
#     `01_Covid_positive_test_Pillar 2`$id), 
#   category.names = c("SGSS" , "Pillar 2 "),
#   # fill = mycolors$color[7:8],
#   fill = pal_sources[pal_sources$data_source == "SGSS" | pal_sources$data_source == "Pillar 2",]$color
# )
```

### `01_Covid_diagnosis`
HES OP has been dropped therefore this is 100% GDPPR at the moment

# `02_Covid_admission`

```{r 02_Covid_admission}
display_venn(
  x = list(`02_Covid_admission_CHESS`$id,
           `02_Covid_admission_HES APC`$id, 
           `02_Covid_admission_SUS`$id),
  category.names = c("CHESS" , "HES APC ", "SUS"),
  fill = pal_sources[pal_sources$data_source == "CHESS" | pal_sources$data_source == "HES APC" | pal_sources$data_source == "SUS",]$color
  )
```

# 03 Critical care

## `03_...`

```{r all_critical_care}
display_venn(
  x = list(
    c(`03_NIV_treatment_CHESS`$id,`03_IMV_treatment_CHESS`$id,`03_ICU_admission_CHESS`$id,`03_ECMO_treatment_CHESS`$id),
    c(`03_NIV_treatment_HES APC`$id,`03_IMV_treatment_HES APC`$id,`03_ECMO_treatment_HES APC`$id),
    c(`03_NIV_treatment_HES CC`$id,`03_IMV_treatment_HES CC`$id,`03_ICU_admission_HES CC`$id),
    c(`03_NIV_treatment_SUS`$id,`03_IMV_treatment_SUS`$id,`03_ECMO_treatment_SUS`$id)
           ),
  category.names = c("CHESS" , "HES APC", "HES CC ", "SUS"),
  fill = pal_sources[pal_sources$data_source == "CHESS" | pal_sources$data_source == "HES APC" | pal_sources$data_source == "HES CC" | pal_sources$data_source == "SUS",]$color
)
```


### `03_NIV_treatment`

```{r 03_NIV_treatment}
display_venn(
  x = list(`03_NIV_treatment_CHESS`$id,
           `03_NIV_treatment_HES APC`$id, 
           `03_NIV_treatment_HES CC`$id,
           `03_NIV_treatment_SUS`$id),
  category.names = c("CHESS" , "HES APC ", "HES CC", "SUS"),
  fill = pal_sources[pal_sources$data_source == "CHESS" | pal_sources$data_source == "HES APC" | pal_sources$data_source == "HES CC" | pal_sources$data_source == "SUS",]$color
  )
```

### `03_IMV_treatment`

```{r 03_IMV_treatment}
display_venn(
  x = list(`03_IMV_treatment_CHESS`$id,
           `03_IMV_treatment_HES APC`$id, 
           `03_IMV_treatment_HES CC`$id,
           `03_IMV_treatment_SUS`$id),
  category.names = c("CHESS" , "HES APC ", "HES CC", "SUS"),
  fill = pal_sources[pal_sources$data_source == "CHESS" | pal_sources$data_source == "HES APC" | pal_sources$data_source == "HES CC" | pal_sources$data_source == "SUS",]$color
  )
```
  
### `03_ICU_admission`  

```{r 03_ICU_admission}
display_venn(
  x = list(`03_ICU_admission_CHESS`$id,
           `03_ICU_admission_HES CC`$id),
  category.names = c("CHESS", "HES CC"),
  fill = pal_sources[pal_sources$data_source == "CHESS" | pal_sources$data_source == "HES CC",]$color
  )
```

### `03_ECMO_treatment`

```{r 03_ECMO_treatment}
display_venn(
  x = list(`03_ECMO_treatment_CHESS`$id,
           `03_ECMO_treatment_HES APC`$id, 
           `03_ECMO_treatment_SUS`$id),
  category.names = c("CHESS" , "HES APC ", "SUS"),
  fill = pal_sources[pal_sources$data_source == "CHESS" | pal_sources$data_source == "HES APC" | pal_sources$data_source == "SUS",]$color
  )
```

# 4 Deaths

NB `04_Fatal_with_covid_diagnosis` and `04_Fatal_without_covid_diagnosis` are entirely derived from `deaths` therefore no value in vennDiagram

## `04_..`
All deaths

```{r alldeaths}
display_venn(
  x = list(
    c(`04_Fatal_with_covid_diagnosis_deaths`$id,`04_Fatal_without_covid_diagnosis_deaths`$id),
     `04_Covid_inpatient_death_HES APC`$id, 
     `04_Covid_inpatient_death_SUS`$id),
  category.names = c("Deaths" , "HES APC ", "SUS"),
  fill = pal_sources[pal_sources$data_source == "Deaths" | pal_sources$data_source == "HES APC" | pal_sources$data_source == "SUS",]$color
  )
```




### `04_Covid_inpatient_death`

```{r 04_Covid_inpatient_death}
display_venn(
  x = list(`04_Fatal_with_covid_diagnosis_deaths`$id,
           `04_Covid_inpatient_death_HES APC`$id, 
           `04_Covid_inpatient_death_SUS`$id),
  category.names = c("Deaths" , "HES APC ", "SUS"),
  fill = pal_sources[pal_sources$data_source == "Deaths" | pal_sources$data_source == "HES APC" | pal_sources$data_source == "SUS",]$color
  )
```