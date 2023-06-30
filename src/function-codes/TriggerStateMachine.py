import json
import boto3
import uuid
import os
import logging
import json
from botocore.exceptions import ClientError
from botocore.client import Config
from urllib import parse

# Enable debugging for troubleshooting
# boto3.set_stream_logger("")


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Define Environmental Variables
my_state_machine_arn = str(os.environ['step_function_arn'])
my_region = str(os.environ['current_region'])

# Setup Service Client
client = boto3.client('stepfunctions', region_name=my_region)

def invoke_state_machine(state_machine_arn, inv_input, invocation_name):
    try:
        response = client.start_execution(
            stateMachineArn=state_machine_arn,
            name=invocation_name,
            input=inv_input,
            # traceHeader='string'
        )
    except ClientError as e:
        logger.error(e)
    except Exception as e:
        logger.error(e)
    else:
        logger.info(f"Invocation Successful")



def lambda_handler(event, context):
    logger.info(event)

    #### Get S3 Event Details #####

    s3Bucket = str(event['Records'][0]['s3']['bucket']['name'])
    s3Key = parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')


    ### Create Dict Input for the State machine invocation ###

    state_machine_dict_input = {
                            's3Bucket': s3Bucket,
                            's3Key': s3Key,
                            }


    # Convert to JSON String
    state_machine_input = json.dumps(state_machine_dict_input)
    logger.info(type(state_machine_input))
    logger.info(state_machine_input)

    my_invocation_name = str(uuid.uuid4())

    # Call State Machine

    invoke_state_machine(my_state_machine_arn, state_machine_input, my_invocation_name)

    # Return Values
    return {
        'statusCode': 200,
        'body': json.dumps('State machine successfully invoked!')
    }
