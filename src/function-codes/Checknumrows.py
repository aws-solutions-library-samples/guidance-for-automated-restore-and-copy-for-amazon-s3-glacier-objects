import math
import json
import os
import uuid
import boto3
from botocore.exceptions import ClientError
import logging
import datetime
from urllib import parse


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


# Define Environmental Variables
my_glue_db = str(os.environ['glue_db'])
my_glue_tbl = str(os.environ['glue_tbl'])
my_workgroup_name = str(os.environ['workgroup_name'])
my_csv_max_rows = int(os.environ['csv_max_rows'])
my_s3_bucket = str(os.environ['s3_bucket'])
my_region = str(os.environ['current_region'])
my_incl_versions = str(os.environ['included_obj_versions'])
my_storage_class_to_restore = str(os.environ['storage_class_to_restore'])


# Set Service Client
athena_client = boto3.client('athena', region_name=my_region)
s3Client = boto3.client("s3", region_name=my_region)


############# Athena Query Function #############
def start_query_execution(query_string, athena_db, workgroup_name):
    logger.info(f'Starting Athena query...... with query string: {query_string}')
    try:
        execute_query = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': athena_db
            },
            WorkGroup=workgroup_name,
        )
    except ClientError as e:
        logger.error(e)
        raise
    else:
        logger.info(f'Query Successful: {execute_query}')
        return execute_query.get('QueryExecutionId')


def lambda_handler(event, context):
    logger.info(f'Event details are: {event}')
    s3Bucket = event.get('s3Bucket')
    s3Key = parse.unquote_plus(event.get('s3Key'))
    my_dt = s3Key.split('/')[-2].split('=')[-1]

    ######  Start Athena Query ######

    ### Condition for storage class to restore ###
    archive_qr = None

    if my_storage_class_to_restore == 'GLACIER':
        archive_qr = f"storage_class = 'GLACIER'"
    elif my_storage_class_to_restore == 'DEEP_ARCHIVE':
        archive_qr = f"storage_class = 'DEEP_ARCHIVE'"
    elif my_storage_class_to_restore == 'GLACIER_AND_DEEP_ARCHIVE':
        archive_qr = f'''(storage_class = 'GLACIER' OR storage_class = 'DEEP_ARCHIVE')'''


    my_query_string = ''

    ### Create Multiple Query String for Versioned and Non-Versioned Restores ###

    my_query_string_no_version = f"""
    SELECT count(*)
    FROM "{my_glue_db}"."{my_glue_tbl}"
    WHERE {archive_qr}
    AND
    is_latest = true
    AND
    is_delete_marker = false
    AND
    bucket = '{my_s3_bucket}'
    AND
    dt = '{my_dt}';
    """

    my_query_string_versioned  = f"""
    SELECT count(*)
    FROM "{my_glue_db}"."{my_glue_tbl}"
    WHERE {archive_qr}
    AND
    is_delete_marker = false
    AND
    bucket = '{my_s3_bucket}'
    AND
    dt = '{my_dt}';
    """

    ###
    if my_incl_versions == 'Current':
        my_query_string = my_query_string_no_version
    elif my_incl_versions == 'All':
        my_query_string = my_query_string_versioned


    try:
        my_query_execution_id = start_query_execution(my_query_string, my_glue_db, my_workgroup_name)
    except Exception as e:
        logger.error(e)
        raise
    return {
            's3Bucket' : s3Bucket,
            's3Key' : s3Key,
            'jobgroupid' : str(uuid.uuid4()),
            'my_dt' : my_dt,
            'my_query_execution_id' : my_query_execution_id,
            'my_s3_bucket': my_s3_bucket,
            }
