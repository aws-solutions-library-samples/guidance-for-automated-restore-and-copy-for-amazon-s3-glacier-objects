import json
import cfnresponse
import logging
import os
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

# Enable debugging for troubleshooting
# boto3.set_stream_logger("")


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])


# Set SDK paramters
config = Config(retries = {'max_attempts': 5})

# Set variables
# Set Service Parameters
s3Client = boto3.client('s3', config=config, region_name=my_region)


def check_bucket_exists(bucket):
    logger.info(f"Checking if Archive Bucket Exists")
    try:
        check_bucket = s3Client.get_bucket_location(
            Bucket=bucket,
        )
    except ClientError as e:
        logger.error(e)
        raise
    else:
        logger.info(f"Bucket {bucket}, exists, proceeding with deployment ...")
        return check_bucket            


def lambda_handler(event, context):
  # Define Environmental Variables
  s3Bucket  = event.get('ResourceProperties').get('bucketexists')

  logger.info(f'Event detail is: {event}')

  if event.get('RequestType') == 'Create':
    # logger.info(event)
    try:
      logger.info("Stack event is Create, checking specified Bucket has Object Lock Enabled...")
      check_bucket_exists(s3Bucket)
      responseData = {}
      responseData['message'] = "Successful"
      logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
      cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
    except Exception as e:
      logger.error(e)
      responseData = {}
      responseData['message'] = str(e)
      failure_reason = str(e) 
      logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
      cfnresponse.send(event, context, cfnresponse.FAILED, responseData, reason=failure_reason)


  elif event.get('RequestType') == 'Delete' or event.get('RequestType') == 'Update':
    logger.info(event)
    try:
      logger.info(f"Stack event is Delete or Update, nothing to do....")
      responseData = {}
      responseData['message'] = "Completed"
      logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
      cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
    except Exception as e:
      logger.error(e)
      responseData = {}
      responseData['message'] = str(e)
      logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
      cfnresponse.send(event, context, cfnresponse.FAILED, responseData)                  
