from urllib import parse
import boto3
import botocore
import os
import json
import logging
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Enable Verbose logging for Troubleshooting
# boto3.set_stream_logger("")

# Define Lambda Environmental Variable
my_role_arn = str(os.environ['batch_ops_role'])
report_bucket_name = str(os.environ['batch_ops_report_bucket'])
# Archive Restoration Details ###############################################
restore_expiration = int(os.environ['archive_restore_days'])
restore_tier = str(os.environ['archive_restore_tier'])
accountId = str(os.environ['my_account_id'])
my_region = str(os.environ['my_current_region'])
my_sns_topic_arn = str(os.environ['my_sns_topic_arn'])
my_s3_bucket = str(os.environ['s3_bucket'])


# Specify variables #############################

# Job Manifest Details ################################
job_manifest_format = 'S3BatchOperations_CSV_20180820'  # S3InventoryReport_CSV_20161130


# Job Report Details ############################
report_prefix = str(os.environ['batch_ops_restore_report_prefix'])
report_format = 'Report_CSV_20180820'
report_scope = 'AllTasks'

# Construct ARNs ############################################
report_bucket_arn = 'arn:aws:s3:::' + report_bucket_name

# Initiate Service Clients ###################
s3Client = boto3.client('s3', region_name=my_region)
s3ControlClient = boto3.client('s3control', region_name=my_region)
sns = boto3.client('sns', region_name=my_region)

# SNS Message Function
def send_sns_message(sns_topic_arn, sns_message):
    logger.info("Sending SNS Notification Message......")
    sns_subject = 'Notification from AutoRestoreMigrate Solution'
    try:
        response = sns.publish(TopicArn=sns_topic_arn, Message=sns_message, Subject=sns_subject)
    except ClientError as e:
        logger.error(e)

# Retrive Manifest ETag
def get_manifest_etag(manifest_s3_bucket, manifest_s3_key):
    # Get manifest key ETag ####################################
    try:
        manifest_key_object_etag = s3Client.head_object(Bucket=manifest_s3_bucket, Key=manifest_s3_key)['ETag']
    except ClientError as e:
        logger.error(e)
    else:
        logger.info(manifest_key_object_etag)
        return manifest_key_object_etag


# S3 Batch Restore Job Function

def s3_batch_ops_restore(manifest_bucket, manifest_key):
    logger.info("Calling the Amazon S3 Batch Operation Restore API")

    # Construct ARNs ############################################
    manifest_bucket_arn = 'arn:aws:s3:::' + manifest_bucket
    manifest_key_arn = 'arn:aws:s3:::' + manifest_bucket + '/' + manifest_key
    # Get manifest key ETag ####################################
    manifest_key_object_etag = get_manifest_etag(manifest_bucket, manifest_key)

    # Set Description #
    my_job_description = f"Restore Job by AutoRestoreMigrate Solution for S3Bucket: {my_s3_bucket}"                

    # Set Manifest format and Specify Manifest Fields #
    manifest_format = None
    manifest_fields = None
    manifest_fields_count = None
    if "restore-and-copy/csv-manifest/with-version-id/" in manifest_key:
        logger.info("Set Format to CSV and Use Version ID in manifest")
        manifest_format = 'S3BatchOperations_CSV_20180820'
        manifest_fields = ['Bucket', 'Key', 'VersionId']
        manifest_fields_count = str(len(manifest_fields))
    elif "restore-and-copy/csv-manifest/no-version-id/" in manifest_key:
        logger.info("Set Format to CSV and Don't use Version ID in Manifest")
        manifest_format = 'S3BatchOperations_CSV_20180820'
        manifest_fields = ['Bucket', 'Key']
        manifest_fields_count = str(len(manifest_fields))

    my_bops_restore_kwargs = {

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
            'ReportScope': report_scope
        },
        'Manifest': {
            'Spec': {
                'Format': manifest_format,
                'Fields': manifest_fields
            },
            'Location': {
                'ObjectArn': manifest_key_arn,
                'ETag': manifest_key_object_etag
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

    try:
        response = s3ControlClient.create_job(**my_bops_restore_kwargs)
        logger.info(f"JobID is: {response['JobId']}")
        logger.info(f"S3 RequestID is: {response['ResponseMetadata']['RequestId']}")
        logger.info(f"S3 Extended RequestID is:{response['ResponseMetadata']['HostId']}")
        return response['JobId']
    except ClientError as e:
        logger.error(e)


def lambda_handler(event, context):
    logger.info(event)
    s3Bucket = str(event['Records'][0]['s3']['bucket']['name'])
    logger.info(s3Bucket)
    s3Key = parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    logger.info(s3Key)
    job_id = s3_batch_ops_restore(s3Bucket, s3Key)
    my_sns_message = f'Restore Job {job_id} Successfully Submitted to Amazon S3 Batch Operation'
    send_sns_message(my_sns_topic_arn, my_sns_message)
