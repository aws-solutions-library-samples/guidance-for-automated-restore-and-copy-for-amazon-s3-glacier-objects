import math
import json
import os
import uuid
import boto3
from botocore.exceptions import ClientError
import logging


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])


# Set Service Client
s3 = boto3.resource('s3', region_name=my_region)


def lambda_handler(event, context):
    logger.info(f'Event detail is: {event}')
    csv_files = []
    item_count = 0
    num_count = 0
    item_loop_status = 'NotStarted'
    bucketname = str(event.get('bucketname'))
    prefix = str(event.get('prefix'))
    jobgroupid = str(event.get('jobgroupid'))
    my_csv_num_rows = str(event.get('my_csv_num_rows'))

    # Instatiate Service API Parameters
    mybucket = s3.Bucket(bucketname)

    #### Initiate List Objects ####
    try:
        all_objs = mybucket.objects.filter(Prefix=prefix)
        obj_keys = [obj.key for obj in all_objs if obj.key.endswith('.csv')]
    except ClientError as e:
        logger.error(e)
        raise
    else:
        csv_files = obj_keys
        item_count = len(csv_files)
        logger.info(f'item_count is: {item_count}')
    #### Start variables ###
    # Return Values
    return {
        'item_count': item_count,
        'item_loop_status': item_loop_status,
        'csv_files': csv_files,
        'num_count': num_count,
        'bucketname': bucketname,
        'jobgroupid': jobgroupid,
        'my_csv_num_rows': my_csv_num_rows
    }
