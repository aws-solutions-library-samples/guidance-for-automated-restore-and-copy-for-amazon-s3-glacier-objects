import math
import json
import logging
import os
import boto3
from botocore.exceptions import ClientError


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])
my_src_bucket = str(os.environ['src_bucket'])
my_sns_topic_arn = str(os.environ['sns_topic_arn'])
my_config_id = str(os.environ['inv_config_id'])

# Define Service Clients
s3client = boto3.client('s3', region_name=my_region)
sns = boto3.client('sns', region_name=my_region)


# SNS Message Function
def send_sns_message(sns_topic_arn, sns_message):
    logger.info("Sending SNS Notification Message......")
    sns_subject = 'Notification from AutoRestoreMigrate Solution'
    try:
        response = sns.publish(TopicArn=sns_topic_arn, Message=sns_message, Subject=sns_subject)
    except ClientError as e:
        logger.error(e)
        raise


def del_inventory_configuration(src_bucket, config_id):
    try:
        logger.info(f"Starting the process to remove the S3 Inventory configuration {config_id}")
        response = s3client.delete_bucket_inventory_configuration(
            Bucket=src_bucket,
            Id=config_id,
        )
    except Exception as e:
        logger.error(e)
        raise
    else:
        logger.info(f"Successfully deleted the S3 Inventory configuration {config_id}")



def lambda_handler(event, context):
    logger.info(f'Event detail is: {event}')
    prefix = str(event.get('output_location_path'))
    s3Bucket = str(event.get('s3Bucket'))
    jobgroupid = str(event.get('jobgroupid'))
    my_csv_num_rows = str(event.get('my_csv_num_rows'))
    # Removing Inventory Configuration from the Archive Bucket #
    del_inventory_configuration(my_src_bucket, my_config_id)
    # Send SNS Message #
    my_sns_message = f'Initiating Restore Workflow for {my_csv_num_rows} Keys in S3Bucket {my_src_bucket} for this JobGroup: {jobgroupid}..'
    send_sns_message(my_sns_topic_arn, my_sns_message)
    # ReturnValues
    return {
        'prefix': prefix,
        's3Bucket' : s3Bucket,
        'jobgroupid': jobgroupid,
        'my_csv_num_rows': my_csv_num_rows,
    }
