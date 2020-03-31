## Purpose and Analytical Goals

Sparkify, a music-streaming provider, hopes to create a data warehouse in the cloud in order to perform analytics on their business processes. By using a cloud host, Sparkify can leverage a cloud provider's services in order to streamline their own in-house IT infrastructure needs, and manage its data warehouse by infrastructure-as-code. Sparkify's business processes are captured as JSON files and consist of (a) event logs that represent song streams, and (b) records of songs. 

We will build a cloud data warehouse that transforms the JSON files into data warehouse records. 


## Design

Currently, the JSON files are stored in AWS S3 buckets. Our design will extract the JSON records from S3 and load it to a two staging tables in Redshift. These staging tables ensure type compliance.  introduce constraints. Afterwards, we load data from the staging tables to their destination analytical tables, which use the dimensional model. In these tables, we introduce constraints as well as optimizations for the distributed cloud environment, such as DISTKEY and SORTKEY. 

The nature of the data from source makes the dimensional model extremely intuitive. Our primary fact of interest is the stream of a single song, so the grain of our fact table represents one song play. The data about songs naturally fills out dimensional data.

A quick note regarding SORTKEY and DISTKEY choices:
Sorting by SONGPLAY_ID in the fact_songplays table is intuitive- we're sorting by the order of the song stream.
In the dim_songs table and the dim_artists table, we use their primary keys SONG_ID and ARTIST_ID as distkeys. This is an optimization- we distribute these two tables to each shard in the cluster. This makes our join operations faster by reducing networking overhead. 
The dim_time table is sorted by timestamp and distrubuted.

![image](./cloud_DW.PNG)


## Running
1. Run set_up.py. This provisions the AWS resources we will need. 
2. Run get_endpoint_roleARN.py in order to print the cluster host ("DWH_ENDPOINT") and roleARN ("DWH_ROLE_ARN"). note that since we create a cluster in set_up.py, the cluster may take some time to provision. Therefore, if this results in " KeyError: 'Endpoint' ", just wait a little bit longer and run this script again. You can log on to your Redshift console to check the status of this cluster. 
3. Record the DWH_ENDPOINT and DWH_ROLE_ARN in your dwh.cfg file. These credentials will be needed to continue. Put DWH_ENDPOINT under DWH 'HOST' and DWH_ROLE_ARN under DWH 'ARN'. Save the config file.
4. Run create_tables.py. 
5. Run etl.py
6. When ready to delete the cluster, run tear_down.py.
