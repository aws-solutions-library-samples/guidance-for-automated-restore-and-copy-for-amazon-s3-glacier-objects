import json
import logging
import os
import datetime
from dateutil.tz import tzlocal
from dateutil import parser
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')
# boto3.set_stream_logger("")

# Set Region #
my_region = str(os.environ['AWS_REGION'])

### Initiate Service Client and DDB Table
dynamodb = boto3.resource('dynamodb', region_name=my_region)
client = boto3.client('lambda', region_name=my_region)

### Initiate Variables ######
table = dynamodb.Table(str(os.environ['job_ddb']))
copy_function_name = str(os.environ['copy_function'])
my_archive_storage_class = str(os.environ['existing_archive_storage_class'])
my_gfr_standard_retrieval_delay = int(os.environ['gfr_standard_retrieval_delay'])
my_gfr_bulk_retrieval_delay = int(os.environ['gfr_bulk_retrieval_delay'])
my_gda_standard_retrieval_delay = int(os.environ['gda_standard_retrieval_delay'])
my_gda_bulk_retrieval_delay = int(os.environ['gda_bulk_retrieval_delay'])

# Other Variables
copy_invocation_type = 'RequestResponse'


# Define Copy Job Initiation Delay parameters based on Archive Class #
# Define Parameters #
standard_restore_copy_job_delay = None
bulk_restore_copy_job_delay = None

if my_archive_storage_class == 'GLACIER':
    standard_restore_copy_job_delay = my_gfr_standard_retrieval_delay
    bulk_restore_copy_job_delay = my_gfr_bulk_retrieval_delay
    logger.info(
        f"Delay time set for Glacier are Std: {standard_restore_copy_job_delay} and Bulk: {bulk_restore_copy_job_delay}")

elif my_archive_storage_class == 'DEEP_ARCHIVE' or 'GLACIER_AND_DEEP_ARCHIVE':
    standard_restore_copy_job_delay = my_gda_standard_retrieval_delay
    bulk_restore_copy_job_delay = my_gda_bulk_retrieval_delay
    logger.info(
        f"Delay time set for Deep_Archive are Std: {standard_restore_copy_job_delay} and Bulk: {bulk_restore_copy_job_delay}")


# Function to Invoke Copy Function Worker
def invoke_function(function_name, invocation_type, payload):
    invoke_response = client.invoke(
        FunctionName=function_name,
        InvocationType=invocation_type,
        Payload=payload,

    )
    response_payload = json.loads(invoke_response['Payload'].read().decode("utf-8"))
    return response_payload

# Scan DynamoDB Table
def scan_table(column_name, column_value):
    projection_expression = "copy_manifest_s3bucket, copy_manifest_skey, restore_date_completed, restore_job_tier, " \
                            "restore_job_id "
    ddb_items = []
    scan_kwargs = {
        'FilterExpression': Key(column_name).eq(column_value),
    }
    try:
        done = False
        begin = None
        while not done:
            if begin:
                scan_kwargs['ExclusiveStartKey'] = begin
            response = table.scan(**scan_kwargs)
            ddb_items.extend(response.get('Items', []))
            begin = response.get('LastEvaluatedKey', None)
            done = begin is None
    except ClientError as e:
        logger.error(e)

    return ddb_items

# Update DynamoDB Table Function
def ddb_update_item(restorejobid, restorejobstatus, updatedval1, updatedval2):
    try:
        update_response = table.update_item(
            Key={
                'restore_job_id': restorejobid,
                'restore_job_status': restorejobstatus
            },
            UpdateExpression='SET copy_job_id = :val1, copy_job_status = :val2',
            ExpressionAttributeValues={
                ':val1': updatedval1,
                ':val2': updatedval2
            },
            ReturnValues="UPDATED_NEW"
        )
        logger.info(update_response.get('Attributes'))
    except ClientError as e:
        logger.error(e)


def lambda_handler(event, context):
    my_column_name = 'copy_job_status'
    my_column_value = 'NotStarted'
    ddb_scan_result = scan_table(my_column_name, my_column_value)
    for data in ddb_scan_result:
        logger.info(data)
        copy_job_manifest_key = data.get('copy_manifest_skey')
        copy_job_manifest_bucket = data.get('copy_manifest_s3bucket')
        restore_job_completion_date = data.get('restore_date_completed')
        restore_job_retrieval_tier = data.get('restore_job_tier')
        restore_jobid = data.get('restore_job_id')
        restorejobstatus = data.get('restore_job_status')
        manifest_flds_num = str(data.get('num_manifest_fields'))
        # Add 48 hours to the restore_job_completion, to allow Glacier Restore Completion
        conv_to_timestamp = parser.parse(restore_job_completion_date)
        if restore_job_retrieval_tier == 'STANDARD':
            offset_hours = standard_restore_copy_job_delay
        elif restore_job_retrieval_tier == 'BULK':
            offset_hours = bulk_restore_copy_job_delay
        copy_job_start = conv_to_timestamp + datetime.timedelta(hours=offset_hours)
        logger.info(f"Restore Job Completion date is: {restore_job_completion_date}")
        logger.info(f"Restore Tier is: {restore_job_retrieval_tier}")
        logger.info(f"Scheduled time for Copy Job Start: {copy_job_start}")
        logger.info(f"The Restore Job ID is: {restore_jobid}")
        my_current_time_now = datetime.datetime.utcnow().replace(tzinfo=tzlocal())
        logger.info(f"My current time is: {my_current_time_now}")
        # Start Copy Function Invoke
        # Generate Payload for Invocation:
        my_payload = {"copymanifestbucket": copy_job_manifest_bucket, "copymanifestkey": copy_job_manifest_key,
                      "restorejobid": restore_jobid, 'nummanifestcols': manifest_flds_num}
        my_payload_json = json.dumps(my_payload)

        if my_current_time_now >= copy_job_start:
            logger.info(
                f"My current time is: {my_current_time_now} and job planned time is {copy_job_start} so, start the "
                f"copy job now!")
            # Start Copy Function Invoke
            invoke_copy_funct = invoke_function(copy_function_name, copy_invocation_type, my_payload_json)
            logger.info(invoke_copy_funct)
            invoke_status_code = invoke_copy_funct.get("statusCode")
            invoke_copy_job_id = invoke_copy_funct.get("body")
            # Change Copy Job Status in DDB to Submitted
            updatedval2 = 'Submitted'
            if invoke_status_code == 200 and invoke_copy_job_id:
                # Update the DDB Table if Job Invocation is Successful
                ddb_update_item(restore_jobid, restorejobstatus, invoke_copy_job_id, updatedval2)

    return {
        'statusCode': 200,
        'body': json.dumps('Successful Invocation!')
    }
