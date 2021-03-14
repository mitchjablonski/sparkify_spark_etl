## Spark Pipeline for the Sparkify Streaming Data Serivce

The Sparkify platform has grown, and we've had to transition to spark for our data platform to keep up with the pace and be able to easily scale with our data.

Our pipeline is relatively simple, consistent of getting our spark client, reading from our logs in s3, doing some basic transformations and writing back to s3.

By transitioning our pipeline to spark, we can be confident that the pipeline will be able to scale, providing the data scientists the data that they require to determine our user base characteristics.

## Our 3 main files in our project are:

### ETL.py

This is our primary script that we utilize in the pipeline.  

It is relatively straightforward, retreiving a spark client, grabbing our song data, doing basic transformations and writing back for our song and artist table.
We then process our log data, generating a user and a time table.  We then read back in our song table, join it, and write our songplay table.

### Run pipeline.ipynb

This is a very basic script that reimplements the ETL pipeline main in a notebook.  Only intent is interaction if required.

###  Spark_exploration.ipynb 

This notebook was what we used for understanding what our data looked like, and how to implement the transformations on a subset of data.

