import json
from urllib import parse
import cfnresponse
import logging
import os
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
import datetime
import time
from dateutil.tz import tzlocal
from datetime import datetime

# Enable debugging for troubleshooting
# boto3.set_stream_logger("")


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])
my_role_arn = str(os.environ['batch_ops_role'])
report_bucket_name = str(os.environ['batch_ops_report_bucket'])
my_s3_bucket = str(os.environ['s3_bucket'])
accountId = str(os.environ['my_account_id'])
my_sns_topic_arn = str(os.environ['my_sns_topic_arn'])


# Specify variables #############################

# Job Manifest Details ################################
job_manifest_format = 'S3InventoryReport_CSV_20211130'
job_manifest_prefix = str(os.environ['batch_ops_manifest_prefix'])

# Job Report Details ############################
report_prefix = str(os.environ['batch_ops_restore_report_prefix'])
report_format = 'Report_CSV_20180820'
report_scope = 'AllTasks'

# Manifest Fields
manifest_fields = ['Bucket', 'Key', 'VersionId']
manifest_fields_count = str(3)


# Construct ARNs

report_bucket_arn = 'arn:aws:s3:::' + report_bucket_name
target_resource_arn = 'arn:aws:s3:::' + report_bucket_name


# Manifest Generator Variable
manifest_gen_filter_storage_class_list = []
my_archive_storage_class = str(os.environ['archive_storage_class'])

if my_archive_storage_class == 'GLACIER':
    manifest_gen_filter_storage_class_list = ['GLACIER']
elif my_archive_storage_class == 'DEEP_ARCHIVE':
    manifest_gen_filter_storage_class_list = ['DEEP_ARCHIVE']
elif my_archive_storage_class == 'GLACIER_AND_DEEP_ARCHIVE':
    manifest_gen_filter_storage_class_list = ['GLACIER', 'DEEP_ARCHIVE']          


# Initiate Service Clients ###################
s3Client = boto3.client('s3', region_name=my_region)
s3ControlClient = boto3.client('s3control', region_name=my_region)
sns = boto3.client('sns', region_name=my_region)

# SNS Message Function
def send_sns_message(sns_topic_arn, sns_message):
    logger.info("Sending SNS Notification Message......")
    sns_subject = 'Notification from AutoRestoreCopy Solution'
    try:
        response = sns.publish(TopicArn=sns_topic_arn, Message=sns_message, Subject=sns_subject)
    except ClientError as e:
        logger.error(e)          


# S3 Batch Operation Restore Function

def s3_batch_ops_restore_manifest_generator(restore_expiration, restore_tier, archive_bucket, archive_bucket_object_prefix, archive_bucket_object_suffix, obj_size_greater_than, obj_size_less_than, obj_created_before_string, obj_created_after_string):
    # Set Job Description 
    my_job_description = f"Restore Job by AutoRestoreCopy Solution for S3Bucket: {archive_bucket}"     
    # Generate ARNs
    archive_bucket_arn = 'arn:aws:s3:::' + archive_bucket       
    my_request_kwargs = {
        'AccountId': accountId,
        'ConfirmationRequired': False,
        'Operation': {
            'S3InitiateRestoreObject': {
                'ExpirationInDays': restore_expiration,
                'GlacierJobTier': restore_tier
            }
        },
        'Report': {
            'Bucket': report_bucket_arn,
            'Format': report_format,
            'Enabled': True,
            'Prefix': report_prefix,
            'ReportScope': 'AllTasks'
        },
        'ManifestGenerator': {
            'S3JobManifestGenerator': {
                'SourceBucket': archive_bucket_arn,
                'ManifestOutputLocation': {
                    'Bucket': report_bucket_arn,
                    'ManifestPrefix': job_manifest_prefix,
                    'ManifestEncryption': {
                        'SSES3': {},
                    },
                    'ManifestFormat': job_manifest_format
                },
                'Filter': {
                    'MatchAnyStorageClass': manifest_gen_filter_storage_class_list
                },
                'EnableManifestOutput': True
            }
        },
        'Priority': 10,
        'RoleArn': my_role_arn,
        'Description' : my_job_description,
        'Tags': [
            {
                'Key': 'auto-restore-copy',
                'Value': manifest_fields_count                          
            },
        ]
    }

    logger.info(my_request_kwargs)
    # Append KeyConstraints values if present
    if archive_bucket_object_prefix or archive_bucket_object_suffix:
        my_request_kwargs['ManifestGenerator']['S3JobManifestGenerator']['Filter']['KeyNameConstraint'] = {}
        if archive_bucket_object_prefix:
            my_request_kwargs['ManifestGenerator']['S3JobManifestGenerator']['Filter']['KeyNameConstraint'][
                'MatchAnyPrefix'] = [archive_bucket_object_prefix, ]
            logger.info(f"Source Prefix is present, modified request kwargs to: {my_request_kwargs}")    
        if archive_bucket_object_suffix:
            my_request_kwargs['ManifestGenerator']['S3JobManifestGenerator']['Filter']['KeyNameConstraint'][
                'MatchAnySuffix'] = [archive_bucket_object_suffix, ]
            logger.info(f"Source object suffix is present, modified request kwargs to: {my_request_kwargs}")   

    # Filter by object sizes
    my_request_kwargs['ManifestGenerator']['S3JobManifestGenerator']['Filter'][
        'ObjectSizeGreaterThanBytes'] = obj_size_greater_than
    logger.info(f"Source object minimum size is present, modified request kwargs to: {my_request_kwargs}")     
    my_request_kwargs['ManifestGenerator']['S3JobManifestGenerator']['Filter'][
        'ObjectSizeLessThanBytes'] = obj_size_less_than 
    logger.info(f"Source object maximum size is present, modified request kwargs to: {my_request_kwargs}")

    # Include date time if specified
    # Convert date string to date time:
    if obj_created_before_string:
        obj_created_before = datetime.strptime(obj_created_before_string, '%Y-%m-%d')
        my_request_kwargs['ManifestGenerator']['S3JobManifestGenerator']['Filter']['CreatedAfter'] = obj_created_before
    if obj_created_after_string:
        obj_created_after = datetime.strptime(obj_created_after_string, '%Y-%m-%d') 
        my_request_kwargs['ManifestGenerator']['S3JobManifestGenerator']['Filter']['CreatedAfter'] = obj_created_after                                                       

    try:
        logger.info(f"Submitting kwargs to S3 Batch Operations: {my_request_kwargs}")
        response = s3ControlClient.create_job(**my_request_kwargs)
        logger.info(f"JobID is: {response['JobId']}")
        logger.info(f"S3 RequestID is: {response['ResponseMetadata']['RequestId']}")
        logger.info(f"S3 Extended RequestID is:{response['ResponseMetadata']['HostId']}")
        return response['JobId']
    except ClientError as e:
        logger.error(e)
        raise


def lambda_handler(event, context):
    logger.info(f'Event detail is: {event}')
    my_copy_destination = None
    # Retrieve Invocation Variables
    # Archive Restoration Details ###############################################
    my_restore_expiration = int(event.get('ResourceProperties').get('MyArchiveRestoreDays'))
    my_restore_tier = str(event.get('ResourceProperties').get('MyArchiveRestoreTier'))

    my_archive_bucket = str(event.get('ResourceProperties').get('MyBucketwithArchives'))

    my_archive_bucket_object_prefix = str(event.get('ResourceProperties').get('ArchiveBucketPrefix'))

    my_archive_bucket_object_suffix = str(event.get('ResourceProperties').get('ArchiveBucketObjectSuffix'))

    my_archive_bucket_object_size_greater_than = int(event.get('ResourceProperties').get('ArchiveBucketObjectSizeGreaterThan'))

    my_archive_bucket_object_size_less_than = int(event.get('ResourceProperties').get('ArchiveBucketObjectSizeLessThan'))

    my_obj_created_before_string = str(event.get('ResourceProperties').get('ArchiveBucketObjectCreatedBefore'))

    my_obj_created_after_string = str(event.get('ResourceProperties').get('ArchiveBucketObjectCreatedAfter'))                              

    my_archive_storage_class = str(event.get('ResourceProperties').get('MyExistingArchiveClass'))

    # Generate ARNs
    my_archive_bucket_arn = 'arn:aws:s3:::' + my_archive_bucket

    # Initiate Custom lambda Invocation based on Stack Request Type
    if event.get('RequestType') == 'Create':
        # logger.info(event)
        try:
            logger.info("Stack event is Create or Update. Initiating Archived objects Restore...")
            # Introduce a delay to allow consistency
            # sleep is included intentionally
            # nosemgrep: arbitrary-sleep
            time.sleep(150)  # nosemgrep: arbitrary-sleep
            s3_batch_ops_restore_manifest_generator(my_restore_expiration, my_restore_tier, my_archive_bucket, my_archive_bucket_object_prefix, my_archive_bucket_object_suffix, my_archive_bucket_object_size_greater_than, my_archive_bucket_object_size_less_than, my_obj_created_before_string, my_obj_created_after_string)
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


    elif event.get('RequestType') == 'Update':
        # logger.info(event)
        try:
            logger.info("Stack event is Create or Update. Initiating Archived objects Restore...")
            s3_batch_ops_restore_manifest_generator(my_restore_expiration, my_restore_tier, my_archive_bucket, my_archive_bucket_object_prefix, my_archive_bucket_object_suffix, my_archive_bucket_object_size_greater_than, my_archive_bucket_object_size_less_than, my_obj_created_before_string, my_obj_created_after_string)
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

    elif event.get('RequestType') == 'Delete':
        logger.info(event)
        try:
            logger.info(f"Stack event is Delete, nothing to do....")
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
