## Project Summary

A fictitious music streaming company has data stored in Amazon S3.  My job as the data engineer is to take that data, load it onto an Amazon Redshift
cluster as staging tables, then execute SQL statements on the staging tables to create fact and dimension tables for analytics on Amazon Redshift.

## Desired Schema for Data Model

> ### Fact Table
>
> 1. songplays:  songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

> ### Dimension Tables
> 
> 1. users:  user_id, first_name, last_name, gender, level
> 2. songs:  song_id, title, artist_id, year, duration
> 3. artists:  artist_id, name, location, lattitude, longitude
> 4. time:  start_time, hour, day, week, month, year, weekday

## Steps taken to complete project

1. Boiler plate code was used to create the Redshift cluster programmatically.
2. After the Redshift cluster was created I could load the data from S3 into staging tables on the cluster. Thus in this step, SQL statements were 
   written to create the staging tables on the cluster. To verify the tabels were created, the Amazon Web Service console was used.
3. Using the COPY command, the data from S3 was copied onto the staging tables.
4. 


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
