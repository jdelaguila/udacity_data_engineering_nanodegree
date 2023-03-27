## Project Summary

A music streaming startup Sparkify has expanded and wants a data engineer to
move their datawarehouse to a data lake. Currently all their data resides in
S3.  This project takes the data from S3, transforms the data into fact and
dimension tables and stores it back on S3.

## How to Run Script

To run the etl.py script AWS Access and Secret keys are needed.  Once
those are obtained, place them without quotation marks in the appropriate 
places in the dl.cfg file. Create a new S3 bucket in us-west-2 region and 
enter the S3 URL in etl.py in the main function for the **output_data** variable.
etl.py can be run in the terminal.

## Files

> ##### dl.cfg
> Configuration file for AWS credentials

> ##### test.ipynb
> Jupyter notebook to test etl as script was being built

> ##### etl.py
> Main script to run that extracts data from S3, transforms it into fact and
> dimension tables, and then loads them back on to S3.

> ##### README.md
> Markdown file explaining project.

> ##### .parquet files
> Files loaded to workspace during testing.