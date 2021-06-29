---
layout: phenotype
title: COVID-19 Hospital Admission
name: Covid_admission
phenotype_id: 
type: Disease or Syndrome
group: Respiratory
data_sources: 
    - Hospital Episode Statistics Admitted Patient Care
    - Secondary Uses Services Payment By Results
clinical_terminologies: 
    - ICD-10
validation:
    - cross-source
codelists:
    - doe_example_123456_term1.csv
    - doe_example_123456_term2.csv
    - doe_example_123456_term3.csv
valid_event_data_range: 23/01/2020 - 31/03/2021
sex: 
    - Female
    - Male
author: 
    - Chris Tomlinson & Johan Thygesen
publications: 
    - Thygesen J. et al. Characterising COVID-19 related events in a nationwide electronic health record cohort of 56 million people in England. Preprint. 2021
status: ALPHA
date: 2021-06-29
modified_date: 2021-06-29
version: 0.1
---

### Data source 1
{% include csv.html csvdata=site.data.codelists.doe_example_123456_term1 %}

### Data source 2 
{% include csv.html csvdata=site.data.codelists.doe_example_123456_term2 %}

### Data source 3
{% include csv.html csvdata=site.data.codelists.doe_example_123456_term3 %}

### Implementation

Cupcake ipsum dolor. Sit amet powder. Biscuit apple pie marshmallow jelly gummi bears. Candy canes cotton candy jujubes. Jelly-o jelly beans sesame snaps. Ice cream pudding pudding halvah sugar plum. Marshmallow cheesecake gingerbread pie gingerbread gummi bears. Gummi bears cotton candy tiramisu caramels jelly oat cake caramels ice cream.

### Evaluation

Cupcake ipsum dolor. Sit amet powder. Biscuit apple pie marshmallow jelly gummi bears. Candy canes cotton candy jujubes. Jelly-o jelly beans sesame snaps. Ice cream pudding pudding halvah sugar plum. Marshmallow cheesecake gingerbread pie gingerbread gummi bears. Gummi bears cotton candy tiramisu caramels jelly oat cake caramels ice cream.
