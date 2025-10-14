# overdoses
analyzing U.S. overdoses and related data  
based on [CDC Provisional Drug Overdose Death Counts](https://www.cdc.gov/nchs/nvss/vsrr/prov-county-drug-overdose.htm)

## data 
see models.py and google drive

## db 
This repository is designed to be equally accessible to users via Python, R, SQLite or spreadsheets

### github
`git pull` to get latest changes  
`git add` any files you are adding  
`git commit -m` to add a message to your commit  
`git push` to commit your changes  

### sqlite
database file is od.db  
archives in data/db_backups and on google drive

### python
use peewee to load and query data

### R
- open RStudio  
- set your working directory, e.g.:   
`setwd('~/Documents/GitHub/overdoses')`  
then,  `source('db.R')`  to access tables:   
  - countys  
  - countymonths  
  - states
  - statemonths
 
e.g. make a scatterplot comparing pct change in overdose mortality (12-month ending 202308 vs 202412) against pct of adults (ages 19-64) on medicaid:  
```
plot(
  counties$pct_chg_202308_202412,
  counties$pct_medicaid_19_64__b27010)
```
![draft scatter of percent change mortality against percent adults on medicaid](docs/od-scatter.png)
