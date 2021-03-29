### PROJECT SUMMARY:
  The project load songs and logs data from given s3 buckets and create the staging tables on Redshift. Once staging tables are up, it extracts data in fact and dimension schema songplay table is a fact table, and songs, users, time, and artists are dimension tables. It defines distribution and sort keys to run analytics faster.
  
### Setup the redshift:  
  Create redshift cluseter and associated I am role, and update the dwf.cfg file.
  
### How to run Project:
  From notebook launcher, launch a terminal and run:
  
  ``` python3 create_tables.py```
  
  ``` python3 etl.py```

### Running a analytics query:
  Run all the cells in query.ipynb

### Screenshot
  Please check screenshot of redshift cluster in screenshot folder.  
  