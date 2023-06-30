import boto3
import botocore
import os
import logging
import datetime
from botocore.exceptions import ClientError
from urllib import parse

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Initiate Variables

# Lambda Environment Variables
accountId = str(os.environ['my_account_id'])
my_region = str(os.environ['my_current_region'])
my_sns_topic_arn = str(os.environ['my_sns_topic_arn'])

# Create Service Clients
dynamodb = boto3.resource('dynamodb', region_name=my_region)
s3ControlClient = boto3.client('s3control', region_name=my_region)
sns = boto3.client('sns', region_name=my_region)
s3Client = boto3.client('s3', region_name=my_region)
# Instantiate Table Resource
table = dynamodb.Table(str(os.environ['job_ddb']))


# SNS Message Function
def send_sns_message(sns_topic_arn, sns_message):
    sns_subject = 'Notification from AutoRestoreMigrate Solution'
    logger.info("Sending SNS Notification Message......")
    try:
        response = sns.publish(TopicArn=sns_topic_arn, Message=sns_message, Subject=sns_subject)
    except ClientError as e:
        logger.error(e)


def create_ddb_entry(
        job_id,
        job_status,
        job_operation,
        job_tier,
        job_arn,
        date_created,
        date_completed,
        number_of_tasks,
        tasks_succeeded,
        tasks_failed,
        bucket_name,
        key_name,
        job_details,
        num_manifest_fields,
        copy_job_status,
):
    logger.info("Create DDB Entry for S3 Batch Operation Job Tracker")
    try:
        response = table.put_item(
            Item={
                'restore_job_id': job_id,
                'restore_job_status': job_status,
                'job_operation': job_operation,
                'restore_job_tier': job_tier,
                'restore_job_arn': job_arn,
                'restore__date_created': date_created,
                'restore_date_completed': date_completed,
                'restore_number_of_tasks': number_of_tasks,
                'restore_tasks_succeeded': tasks_succeeded,
                'restore_tasks_failed': tasks_failed,
                'copy_job_status': copy_job_status,
                'copy_manifest_s3bucket': bucket_name,
                'copy_manifest_skey': key_name,
                'restore_job_details': job_details,
                'num_manifest_fields': num_manifest_fields,
            }
        )
        logger.info("PutItem succeeded:")
    except ClientError as e:
        print(e)


def s3_batch_describe_job(my_job_id):
    response = s3ControlClient.describe_job(
        AccountId=accountId,
        JobId=my_job_id
    )
    job_desc = (response.get('Job'))
    return job_desc


def ddb_update_item(restorejobid, restorejobstatus, updatedval1, updatedval2, updatedval3, updatedval4, updatedval5, updatedval6):
    try:
        update_response = table.update_item(
            Key={
                'restore_job_id': restorejobid,
                'restore_job_status': restorejobstatus
            },
            UpdateExpression='SET copy_job_status = :val1, copy_number_of_tasks = :val2, copy_tasks_failed = :val3, '
                             'copy_tasks_succeeded = :val4, copy_job_details = :val5, item_expiration = :val6',
            ExpressionAttributeValues={
                ':val1': updatedval1,
                ':val2': updatedval2,
                ':val3': updatedval3,
                ':val4': updatedval4,
                ':val5': updatedval5,
                ':val6': updatedval6
            },
            ReturnValues="UPDATED_NEW"
        )
        logger.info(update_response.get('Attributes'))
    except ClientError as e:
        logger.error(e)


def get_job_tagging(bops_job_id):
    logger.info("Initiate GetJob Tagging")
    try:
        get_job_tag_response = s3ControlClient.get_job_tagging(
            AccountId=accountId,
            JobId=bops_job_id
        )
    except ClientError as e:
        logger.error(e)
    else:
        logger.info("Successfully retrieved Job Tags")
        tag_key = get_job_tag_response.get('Tags')[0].get('Key')
        tag_value = get_job_tag_response.get('Tags')[0].get('Value')
        return tag_key, tag_value


def lambda_handler(event, context):
    logger.info(event)
    try:
        s3Bucket = str(event['Records'][0]['s3']['bucket']['name'])
        s3Key = parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        logger.info(f"S3 Key is: {s3Key}")
        retrieve_job_id = s3Key.split('/')[-2]
        job_id = retrieve_job_id.replace('job-', '', 1)
        my_job_details = s3_batch_describe_job(job_id)
        logger.info(f"Batch Operation Job details: {my_job_details}")
        job_operation = list(my_job_details.get('Operation').keys())[0]
        job_status = my_job_details.get('Status')
        job_arn = my_job_details.get('JobArn')
        job_creation_datetime = str(my_job_details.get('CreationTime'))
        job_completion_datetime = str(my_job_details.get('TerminationDate'))
        number_of_tasks = my_job_details.get('ProgressSummary').get('TotalNumberOfTasks')
        number_of_fields = str(len(my_job_details.get('Manifest').get('Spec').get('Fields')))
        tasks_succeeded = my_job_details.get('ProgressSummary').get('NumberOfTasksSucceeded')
        tasks_failed = my_job_details.get('ProgressSummary').get('NumberOfTasksFailed')
        logger.info(f'Number of Tasks: {number_of_tasks}')
        logger.info(f'Tasks_succeeded: {tasks_succeeded}')
        logger.info(f'Tasks_failed: {tasks_failed}')
        # Set DDB Item Expiration to 60 days #
        num_days = 60
        my_item_expiration = int((datetime.datetime.now() + datetime.timedelta(num_days)).timestamp())
        # Set DDB Entry Status for Jobs based on Successful or Failed Status
        if job_status == 'Complete':
            if number_of_tasks == tasks_failed:
                set_copy_job_status = 'DoNotProceed'
                my_sns_message = f'All Tasks Failed! Please check the Batch Operations Job JobID {job_id} Completion Report in the Amazon S3 Console for more details.'
                send_sns_message(my_sns_topic_arn, my_sns_message)
            else:
                set_copy_job_status = 'NotStarted'
        job_details = str(my_job_details)
        # Only work on Tagged Jobs
        job_tag_key, job_tag_value = get_job_tagging(job_id)
        # Workflow for a Restore Job Creates an Entry in DynamoDB, Copy Job Updates existing Table ########
        if job_operation == 'S3InitiateRestoreObject':
            job_tier = my_job_details.get('Operation').get('S3InitiateRestoreObject').get('GlacierJobTier')
            logger.info(f"Restore Job Tier is: {job_tier}")
            # Starting Condition
            if job_tag_key == 'auto-restore-copy' and job_status == 'Complete':
                my_sns_message = f'Restore Job {job_id} Completed: {tasks_failed} failed out of {number_of_tasks}. Please check the Batch Operations Job JobID {job_id} in the Amazon S3 Console for more details.'
                send_sns_message(my_sns_topic_arn, my_sns_message)
                create_ddb_entry(
                    job_id,
                    job_status,
                    job_operation,
                    job_tier,
                    job_arn,
                    job_creation_datetime,
                    job_completion_datetime,
                    number_of_tasks,
                    tasks_succeeded,
                    tasks_failed,
                    s3Bucket,
                    s3Key,
                    job_details,
                    number_of_fields,
                    set_copy_job_status
                )

            elif job_tag_key == 'auto-restore-copy' and job_status == 'Failed':
                my_sns_message = f'Restore Job {job_id} failed, please check the Batch Operations Job JobID {job_id} in the Amazon S3 Console for more details!'
                send_sns_message(my_sns_topic_arn, my_sns_message)

        elif job_operation == 'LambdaInvoke':
            if job_tag_key == 'auto-restore-copy' and job_status == 'Complete':
                logger.info("Updating the Database with Copy Job information!")
                my_sns_message = f'Copy Job {job_id} Completed: {tasks_failed} failed out of {number_of_tasks}. Please check the Batch Operations Job JobID {job_id} in the Amazon S3 Console for more details.'
                send_sns_message(my_sns_topic_arn, my_sns_message)
                ddb_update_item(job_tag_value, 'Complete', 'Complete', number_of_tasks, tasks_failed, tasks_succeeded,
                                job_details, my_item_expiration)

            elif job_tag_key == 'auto-restore-copy' and job_status == 'Failed':
                my_sns_message = f'Copy Job {job_id} failed, please check the Batch Operations Job JobID {job_id} in the Amazon S3 Console for more details!'
                send_sns_message(my_sns_topic_arn, my_sns_message)

    except Exception as e:
        logger.error(e)
        raise
