import math
import json
import logging
import os
import boto3
from botocore.exceptions import ClientError


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')
# boto3.set_stream_logger("")

# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])
restore_function_name = str(os.environ['restore_function'])
my_incl_versions = str(os.environ['included_obj_versions'])


# Other Variables
copy_invocation_type = 'RequestResponse'

### Initiate Service Client
client = boto3.client('lambda', region_name=my_region)


# Function to Invoke Copy Function Worker
def invoke_function(function_name, invocation_type, payload):
    try:
        invoke_response = client.invoke(
          FunctionName=function_name,
          InvocationType=invocation_type,
          Payload=payload,

        )
    except ClientError as e:
        logger.error(e)
        raise
    else:
        response_payload = json.loads(invoke_response['Payload'].read().decode("utf-8"))
        return response_payload



def lambda_handler(event, context):
    logger.info(f'Event details: {event}')
    keyname = None
    num_fields = None
    item_count = int(event.get('item_count'))
    item_loop_status = 'Started'
    csv_files = event.get('csv_files')
    num_count = int(event.get('num_count'))
    bucketname = event.get('bucketname')
    jobgroupid = str(event.get('jobgroupid'))
    my_csv_num_rows = str(event.get('my_csv_num_rows'))
    logger.info(f"Item count is: {item_count}")
    logger.info(f"num_count is: {num_count}")


    # Check Execution Flow:
    if num_count == item_count:
        item_loop_status = 'complete'
        logger.info(f'item_loop_status is: {item_loop_status}')
    else:
        keyname = csv_files[num_count]
        logger.info(f'keyname is: {keyname}')
        # Generate Payload for Invocation:
        if my_incl_versions == 'Current':
            num_fields = 2
        elif my_incl_versions == 'All':
            num_fields = 3

        my_payload = {
            "Records": [{
                "s3": {
                    "bucket": {
                        "name": bucketname
                    },
                    "object": {
                        "key": keyname
                    }
                },
                "jobspec": {
                    "fields": num_fields
                },
                "jobgroupid": jobgroupid,
            }]
        }

        my_payload_json = json.dumps(my_payload)
        logger.info(my_payload)
        logger.info(my_payload_json)

        # Start Copy Function Invoke
        invoke_restore_funct = invoke_function(restore_function_name, copy_invocation_type, my_payload_json)
        logger.info(invoke_restore_funct)
        invoke_status_code = invoke_restore_funct.get("statusCode")
        invoke_copy_job_id = invoke_restore_funct.get("body")

        num_count += 1

    # Return Values
    return {
        'item_count': item_count,
        'item_loop_status': item_loop_status,
        'csv_files': csv_files,
        'num_count': num_count,
        'bucketname': bucketname,
        'keyname': keyname,
        'jobgroupid': jobgroupid,
        'my_csv_num_rows': my_csv_num_rows,
    }
