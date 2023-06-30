import math
import json
import os
import uuid
import boto3
from botocore.exceptions import ClientError
import logging
from urllib.parse import urlparse


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


# Define Environmental Variables
my_glue_db = str(os.environ['glue_db'])
my_glue_tbl = str(os.environ['glue_tbl'])
my_workgroup_name = str(os.environ['workgroup_name'])
my_region = str(os.environ['current_region'])
my_incl_versions = str(os.environ['included_obj_versions'])
my_storage_class_to_restore = str(os.environ['storage_class_to_restore'])


# Set Service Client
athena_client = boto3.client('athena', region_name=my_region)
s3Client = boto3.client("s3", region_name=my_region)


############# Athena Query Function #############

def start_query_execution(query_string, athena_db, workgroup_name, query_output_location):
    logger.info(f'Starting Athena query...... with query string: {query_string}')
    try:
        execute_query = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': athena_db
            },
            ResultConfiguration={
                'OutputLocation': query_output_location,
            },
            WorkGroup=workgroup_name,
        )
    except ClientError as e:
        logger.error(e)
        raise
    else:
        logger.info(f'Query Successful: {execute_query}')


def lambda_handler(event, context):
    logger.info(f'Initiating Main Function...')
    print(event)
    s3Bucket = str(event.get('s3Bucket'))
    my_s3_bucket = str(event.get('my_s3_bucket'))
    chunk_num = int(event.get('next_chunk'))
    next_chunk = int(event.get('next_chunk'))
    num_chunks = int(event.get('num_chunks'))
    my_csv_max_rows = int(event.get('my_csv_max_rows'))
    my_csv_num_rows = int(event.get('my_csv_num_rows'))
    csv_chunking_complete = event.get('csv_chunking_complete')
    my_dt = event.get('my_dt')
    jobgroupid = event.get('jobgroupid')
    print(next_chunk)

    ########## Define Athena Query ##########
    my_query_string = ''

    ### Condition for storage class to restore ###
    archive_qr = None

    if my_storage_class_to_restore == 'GLACIER':
        archive_qr = f"storage_class = 'GLACIER'"
    elif my_storage_class_to_restore == 'DEEP_ARCHIVE':
        archive_qr = f"storage_class = 'DEEP_ARCHIVE'"
    elif my_storage_class_to_restore == 'GLACIER_AND_DEEP_ARCHIVE':
        archive_qr = f'''(storage_class = 'GLACIER' OR storage_class = 'DEEP_ARCHIVE')'''

    my_query_output_location = f's3://{s3Bucket}/athena-query-results/csv-chunks/{jobgroupid}/'
    output_location_path = f'athena-query-results/csv-chunks/{jobgroupid}/'

    ### Create Multiple Query String for Versioned and Non-Versioned Restores ###

    my_query_string_no_version = f"""
    SELECT bucket as "{my_s3_bucket}", key as "my_key"
    FROM "{my_glue_db}"."{my_glue_tbl}"
    WHERE {archive_qr}
    AND
    is_latest = true
    AND
    is_delete_marker = false
    AND
    bucket = '{my_s3_bucket}'
    AND
    dt = '{my_dt}'
    ORDER BY last_modified_date ASC
    OFFSET {next_chunk * my_csv_max_rows}
    LIMIT {my_csv_max_rows};
    """

    my_query_string_versioned  = f"""
    SELECT bucket as "{my_s3_bucket}", key as "my_key", CASE WHEN version_id IS NULL THEN 'null' ELSE version_id END as VersionId
    FROM "{my_glue_db}"."{my_glue_tbl}"
    WHERE {archive_qr}
    AND
    is_delete_marker = false
    AND
    bucket = '{my_s3_bucket}'
    AND
    dt = '{my_dt}'
    ORDER BY last_modified_date ASC
    OFFSET {next_chunk * my_csv_max_rows}
    LIMIT {my_csv_max_rows};
    """

    ### Create Multiple Queries for Current and All Versions ###

    if my_incl_versions == 'Current':
        my_query_string = my_query_string_no_version
    elif my_incl_versions == 'All':
        my_query_string = my_query_string_versioned

    logger.info(my_query_string)

    try:
        start_query_execution(my_query_string, my_glue_db, my_workgroup_name, my_query_output_location)
    except Exception as e:
        logger.error(e)
        raise
    if next_chunk == num_chunks:
        csv_chunking_complete = True
    else:
        next_chunk = chunk_num + 1

    return {
            'num_chunks' : num_chunks,
            'next_chunk': next_chunk,
            'csv_chunking_complete': csv_chunking_complete,
            'my_csv_num_rows' : my_csv_num_rows,
            'my_csv_max_rows' : my_csv_max_rows,
            'my_dt' : my_dt,
            's3Bucket' : s3Bucket,
            'jobgroupid' : jobgroupid,
            'my_query_output_location': my_query_output_location,
            'output_location_path': output_location_path,
            'my_s3_bucket': my_s3_bucket,
            }
