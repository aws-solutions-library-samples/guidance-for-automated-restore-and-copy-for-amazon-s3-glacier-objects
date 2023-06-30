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

############### Athena Get Query Results #######################

def get_query_result(query_execution_id):
    logger.info(f'Getting Athena query results')
    try:
        get_query_results = athena_client.get_query_results(
            QueryExecutionId=query_execution_id,
        )
    except ClientError as e:
        logger.error(e)
        raise
    else:
        logger.info(f'Successful')
        logger.info(get_query_results)
        query_result_count = int(get_query_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        logger.info(get_query_results)
        return query_result_count



def lambda_handler(event, context):
    logger.info(event)
    my_csv_num_rows = None
    num_chunks = None
    next_chunk = 0


    ### Retrieve Required Parameters from Event ###

    my_query_execution_id = str(event.get('my_query_execution_id'))
    s3Bucket = str(event.get('s3Bucket'))
    jobgroupid = event.get('jobgroupid')
    my_dt = event.get('my_dt')

    try:
        my_csv_num_rows = get_query_result(my_query_execution_id)
    except Exception as e:
        logger.error(e)
        raise
    else:
        num_chunks = my_csv_num_rows // my_csv_max_rows
        logger.info(num_chunks)
    return {
            'num_chunks' : num_chunks,
            'my_csv_num_rows' : my_csv_num_rows,
            'csv_chunking_complete': False,
            'next_chunk' : next_chunk,
            'my_csv_max_rows' : my_csv_max_rows,
            's3Bucket' : s3Bucket,
            'jobgroupid' : jobgroupid,
            'my_dt' : my_dt,
            'my_s3_bucket': my_s3_bucket,
            }
