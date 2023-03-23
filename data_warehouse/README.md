## Project Summary

The music streaming startup, Sparkify has grown and wants to take their user and song data and processes to the cloud. Their data currently resides in S3, and as the data engineer, I am create an ETL pipeline that takes this data from S3, stages the data in redshift, and then transform this data into fact and dimension tables that will be useful for business queries.

## Instructions to run scripts

***NOTE: When retrieving information from AWS make sure to select 'us-west-2' region when applicable.***

1. Sign in to AWS console and in the IAM dashboard create a user to obtain an Access Key ID (KEY), and Secret Access Key (SECRET).  
2. Use above information in *dwh.cfg* to fill in KEY and SECRET.
3. Also in *dwh.cfg* fill in DWH_IAM_ROLE_NAME, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD.
4. Save the *dwh.cfg* file and run *create_cluster.py*
5. In your AWS console, navigate to IAM dashboard, click on roles and find the role name you used in DWH_IAM_ROLE_NAME. Click on that role and copy ARN.
6. Paste ARN in [IAM_ROLE] section of *dwh.cfg*.
7. On AWS console, navigate to Amazon Redshift and wait for the newly created cluster to become 'Available.' Be sure to look at 'us-west-2' region. Once available click on the cluster to view general information and copy the endpoint omitting the end port and database name so that the endpoint ends in 'amazonaws.com'
8. Paste the endpoint into HOST in *dwh.cfg* and fill out the rest of [CLUSTER] section with the same information from the [DWH] section.
9. Save *dwh.cfg* and run *create_tables.py*
10. Once *create_tables.py* finishes, run *etl.py*. Queries can be made in the Redshift dashboard after data is loaded.
11. Delete cluster on Redshift dashboard when finished.

## Files submitted

- *create_cluster.py*: creates the redshift cluster on AWS.
- *create_tables.py*: connect to cluster and if staging, fact, and dimension tables already exist, they are dropped and recreated on the redshift cluster.
- *dwh.cfg*: configuration information needed to create and connect to cluster.
- *etl.py*: loads staging tables from S3, then inserts data into fact and dimension tables.
- *sql_queries.py*: contains SQL code to drop tables, create tables, and load tables with data.
- *test.ipynb*: jupyter notebook used to run files.

## Example queries

#### What are the locations that contain the highest number of users?

    SELECT COUNT(DISTINCT u.user_id), s.location
    FROM users u
    JOIN songplays s ON s.user_id = u.user_id
    GROUP BY s.location
    ORDER BY 1 DESC;
    
#### Who is the most listened to artist?

    SELECT COUNT(*), s.artist_id, a.name
    FROM songplays s
    JOIN artists a ON s.artist_id = a.artist_id
    GROUP BY 2, 3
    ORDER BY 1 DESC;
    
#### List all users who listen to a certain artist.

    SELECT DISTINCT u.user_id, u.first_name, u.last_name
    FROM users u 
    JOIN songplays s ON u.user_id = s.user_id
    JOIN artists a ON a.artist_id = s.artist_id
    WHERE a.name = <artist name>;