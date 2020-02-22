## Recommended_pyspark
Extraction of recommended works using work reviews

## Description
works on pyspark.  
Extract recommended works of each user based on the ecaluation of works taken from postgresql[table:Review(id,user_name_id,contents_id,review)].  
It used the ALS optimal algorithm,which is the MF method on the ML package.  
Save 30 contents recommended for each user to Postgresql[table:review_rating(id,user,content,rating)].  

## Usage
OS : macOS Mojave  
Spark : 2.4  
Postgresql : 11.5  
* setting DATABASE_URL
`export DATABASE_URL=postgresql://<USERNAME>@<SERVER_URL>:<PORT>/<DATABASENAME>`  
* Starting the Postgresql
`postgres -D /usr/local/var/postgres`
* Runnning the program
`$SPARK_HOME/bin/spark-submit Program.py`

## Author
shimoyama

