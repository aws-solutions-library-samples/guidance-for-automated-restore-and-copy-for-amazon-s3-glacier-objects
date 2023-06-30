import json
import boto3
import os
import cfnresponse
import logging
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

SUCCESS = "SUCCESS"
FAILED = "FAILED"

### Define Environmental Variables ###
# Define Region
my_region = str(os.environ['AWS_REGION'])
my_config_id = str(os.environ['inv_config_id'])

# Create Service Client
s3 = boto3.resource('s3', region_name=my_region)
s3client = boto3.client('s3', region_name=my_region)


# Remove S3 Inventory Configuration #
def del_inventory_configuration(src_bucket, config_id):
    try:
        logger.info(f"Starting the process to remove the S3 Inventory configuration {config_id}")
        response = s3client.delete_bucket_inventory_configuration(
            Bucket=src_bucket,
            Id=config_id,
        )
    except Exception as e:
        logger.error(e)
    else:
        logger.info(f"Successfully deleted the S3 Inventory configuration {config_id}")

def lambda_handler(event, context):
    my_src_bucket = event['ResourceProperties']['MyBucketwithArchives']
    logger.info("Received event: " + json.dumps(event, indent=2))
    responseData={}
    try:
        if event['RequestType'] == 'Delete':
            logger.info(f"Request Type is {event['RequestType']}")
            logger.info("Inventory configuration deletion is being initiated....!")
            del_inventory_configuration(my_src_bucket, my_config_id)
            logger.info("Sending response to custom resource after Delete")
            responseData['message'] = "Successful"
            logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
            cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)                        
        elif event['RequestType'] == 'Create' or event['RequestType'] == 'Update':
            logger.info(f"Request Type is {event['RequestType']}")
            logger.info("No Action Required, Inventory PutConfiguration is handled by Another Function!")
            logger.info("Sending Successful response to custom resource")
            responseData['message'] = "Successful"
            logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
            cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
    except Exception as e:
        logger.error(f'Deployment failed, see error details: {e}')
        responseStatus = 'FAILED'
        responseData = {'Failure': 'Deployment Failed!'}
        failure_reason = str(e) 
        cfnresponse.send(event, context, responseStatus, responseData, reason=failure_reason)              
