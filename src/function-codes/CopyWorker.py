import json
import json
import logging
import os
import boto3
import botocore
import jmespath
from botocore.exceptions import ClientError


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')
# boto3.set_stream_logger("")

# Define Lambda Environmental Variable
my_role_arn = str(os.environ['batch_ops_role'])
report_bucket_name = str(os.environ['batch_ops_copy_report_bucket'])
bops_invoke_function_arn = str(os.environ['batch_ops_invoke_lambda'])
report_prefix = str(os.environ['batch_ops_copy_report_prefix'])
accountId = str(os.environ['my_account_id'])
my_region = str(os.environ['my_current_region'])
my_sns_topic_arn = str(os.environ['my_sns_topic_arn'])
my_s3_bucket = str(os.environ['s3_bucket'])


# Specify variables #############################

# Job Manifest Details ################################
job_manifest_format = 'S3BatchOperations_CSV_20180820'  # S3InventoryReport_CSV_20161130


# Job Report Details ############################
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
    sns_subject = 'Notification from AutoRestoreMigrate Solution'
    logger.info("Sending SNS Notification Message......")
    try:
        response = sns.publish(TopicArn=sns_topic_arn, Message=sns_message, Subject=sns_subject)
    except ClientError as e:
        logger.error(e)


def lambda_handler(event, context):
    logger.info(event)
    manifest_s3Bucket = event.get('copymanifestbucket')
    manifest_s3Key = event.get('copymanifestkey')
    restore_job_id = event.get('restorejobid')
    manifest_num_flds = str(event.get('nummanifestcols'))
    logger.info(manifest_s3Bucket)
    logger.info(manifest_s3Key)
    logger.info(restore_job_id)
    read_s3_object = get_read_s3_manifest(manifest_s3Bucket, manifest_s3Key)
    copy_job_id_list = []
    if read_s3_object:
        for r in read_s3_object:
            mybucket = r[0]
            mykey = r[1]
            logger.info(mybucket)
            logger.info(mykey)
            copy_job_id = s3_batch_ops_copy(mybucket, mykey, restore_job_id, manifest_num_flds)
            copy_job_id_list.append(copy_job_id)
    else:
        logger.info("All Tasks have failed")

    my_sns_message = f'Copy Job {copy_job_id_list} Successfully Submitted to Amazon S3 Batch Operation'
    send_sns_message(my_sns_topic_arn, my_sns_message)


    # Return Successful Response and JobID Information to Job Scheduler
    return {
        'statusCode': 200,
        'body': copy_job_id_list,
     }


def get_read_s3_manifest(bucket, key):
    result_query = jmespath.compile("Results[?TaskExecutionStatus=='succeeded'].[Bucket,Key]")
    get_response = s3Client.get_object(
        Bucket=bucket,
        Key=key,
    )
    display_s3_object = json.loads(get_response.get('Body').read().decode('utf-8'))
    filtered_result = result_query.search(display_s3_object)
    return filtered_result


def s3_batch_ops_copy(manifest_bucket, manifest_key, restore_job_to_tag, manifest_flds_num):
    if manifest_flds_num == '3':
        manifest_fields = ['Bucket', 'Key', 'VersionId', 'Ignore', 'Ignore', 'Ignore', 'Ignore']
    elif manifest_flds_num == '2':
        manifest_fields = ['Bucket', 'Key', 'Ignore', 'Ignore', 'Ignore', 'Ignore', 'Ignore']

    # Set Description #
    my_job_description = f"Lambda Invoke Copy Job by AutoRestoreMigrate Solution for S3Bucket: {my_s3_bucket}"    

    # Construct ARNs ############################################
    manifest_bucket_arn = 'arn:aws:s3:::' + manifest_bucket
    manifest_key_arn = 'arn:aws:s3:::' + manifest_bucket + '/' + manifest_key
    # Get manifest key ETag ####################################
    manifest_key_object_etag = s3Client.head_object(Bucket=manifest_bucket, Key=manifest_key)['ETag']
    logger.info(manifest_key_object_etag)

    try:
        response = s3ControlClient.create_job(
            AccountId=accountId,
            ConfirmationRequired=False,
            Operation={
                'LambdaInvoke': {
                    'FunctionArn': bops_invoke_function_arn
                }
            },
            Report={
                'Bucket': report_bucket_arn,
                'Format': report_format,
                'Enabled': True,
                'Prefix': report_prefix,
                'ReportScope': report_scope
            },
            Manifest={
                'Spec': {
                    'Format': job_manifest_format,
                    'Fields': manifest_fields
                },
                'Location': {
                    'ObjectArn': manifest_key_arn,
                    'ETag': manifest_key_object_etag
                }
            },
            Priority=10,
            RoleArn=my_role_arn,
            Description=my_job_description,
            Tags=[
                {
                    'Key': 'auto-restore-copy',
                    'Value': restore_job_to_tag
                },
            ]
        )
        logger.info(f"JobID is: {response.get('JobId')}")
        logger.info(f"S3 RequestID is: {response.get('ResponseMetadata').get('RequestId')}")
        logger.info(f"S3 Extended RequestID is:{response.get('ResponseMetadata').get('HostId')}")
        return response['JobId']
    except ClientError as e:
        logger.error(e)
