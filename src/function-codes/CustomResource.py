import json
import boto3
import os
import cfnresponse
import logging
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Define Region
my_region = str(os.environ['AWS_REGION'])

# Create Service Client
s3 = boto3.resource('s3', region_name=my_region)
s3client = boto3.client('s3', region_name=my_region)

### Define Environmental Variables ###
my_inv_schedule = str(os.environ['inv_report_schedule'])
accountId = str(os.environ['account_id'])
my_config_id = str(os.environ['inv_config_id'])

# Define other parameters
my_incl_versions = 'All'

def config_s3_inventory(src_bucket, config_id, dst_bucket,
                        inv_format, src_prefix, dst_prefix, inv_status, inv_schedule, incl_versions):

    ## Generate default kwargs ##
    my_request_kwargs = {
        'Bucket': src_bucket,
        'Id': config_id,
        'InventoryConfiguration': {
            'Destination': {
                'S3BucketDestination': {
                    # 'AccountId': account_id,
                    'Bucket': f'arn:aws:s3:::{dst_bucket}',
                    'Format': inv_format,
                    'Prefix': dst_prefix,
                    'Encryption': {
                        'SSES3': {}
                    }
                }
            },
            'IsEnabled': inv_status,
            'Filter': {
                'Prefix': src_prefix
            },
            'Id': config_id,
            'IncludedObjectVersions': incl_versions,
            'OptionalFields': [
                'Size',
                'LastModifiedDate',
                'StorageClass',
                'ETag',
                'IsMultipartUploaded',
                'ReplicationStatus',
                'EncryptionStatus',
                'ObjectLockRetainUntilDate',
                'ObjectLockMode',
                'ObjectLockLegalHoldStatus',
                'IntelligentTieringAccessTier',
            ],
            'Schedule': {
                'Frequency': inv_schedule
            }
        }
    }

    ## Remove Prefix Parameter if No Value is Specified, All Bucket ##

    logger.info(src_prefix)
    if src_prefix == '' or src_prefix is None:
        logger.info(f'removing filter parameter')
        my_request_kwargs['InventoryConfiguration'].pop('Filter')
        logger.info(f"Modify kwargs no prefix specified: {my_request_kwargs}")

    # Initiating Actual PutBucket Inventory API Call ##
    try:
        logger.info(f'Applying inventory configuration to S3 bucket {src_bucket}')
        s3client.put_bucket_inventory_configuration(**my_request_kwargs)
    except Exception as e:
        logger.error(f'An error occurred processing, error details are: {e}')
        raise


def lambda_handler(event, context):
    my_inv_format = 'Parquet'
    my_dest_prefix = accountId
    my_inv_status = True
    logger.info("Received event: " + json.dumps(event, indent=2))
    responseData={}
    try:
        if event['RequestType'] == 'Delete':
            logger.info(f"Request Type is {event['RequestType']}")
            logger.info("No Action Required, Inventory deletion is handled by Workflow!")
            logger.info("Sending response to custom resource after Delete")
            responseData['message'] = "Successful"
            logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
            cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)                        
        elif event['RequestType'] == 'Create' or event['RequestType'] == 'Update':
            logger.info(f"Request Type is {event['RequestType']}")
            my_src_bucket = event['ResourceProperties']['MyBucketwithArchives']
            my_src_prefix = event['ResourceProperties']['ArchiveBucketPrefix']
            my_dst_bucket = event['ResourceProperties']['MyS3InventoryDestinationBucket']
            config_s3_inventory(my_src_bucket, my_config_id, my_dst_bucket,
                                    my_inv_format, my_src_prefix, my_dest_prefix, my_inv_status, my_inv_schedule, my_incl_versions)
            logger.info("Sending Successful response to custom resource")
            responseData['message'] = "Successful"
            logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
            cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)                        
    except Exception as e:
            responseData['message'] = str(e)
            failure_reason = str(e) 
            logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
            cfnresponse.send(event, context, cfnresponse.FAILED, responseData, reason=failure_reason)                                          
