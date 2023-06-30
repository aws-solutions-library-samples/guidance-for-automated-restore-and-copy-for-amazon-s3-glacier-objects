import cfnresponse
import logging
import random
import boto3
import os
import uuid
import jmespath
from botocore.exceptions import ClientError as ServicesClientError

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Enable Verbose logging for Troubleshooting
# boto3.set_stream_logger("")

# Set Region #
my_region = str(os.environ['AWS_REGION'])

### Initiate Variables ######
# Start Global Variables
my_event_one_id = str(os.environ['event_one_id'])
my_event_one_prefix_value = str(os.environ['event_one_prefix_value'])
my_event_one_suffix_value = str(os.environ['event_one_suffix_value'])

my_event_two_id = str(os.environ['event_two_id'])
my_event_two_prefix_value = str(os.environ['event_two_prefix_value'])
my_event_two_suffix_value = str(os.environ['event_two_suffix_value'])

my_event_three_id = str(os.environ['event_three_id'])
my_event_three_prefix_value = str(os.environ['event_three_prefix_value'])
my_event_three_suffix_value = str(os.environ['event_three_suffix_value'])

my_event_four_id = str(os.environ['event_four_id'])
my_event_four_prefix_value = str(os.environ['event_four_prefix_value'])
my_event_four_suffix_value = str(os.environ['event_four_suffix_value'])


struct_value_1 = str(os.environ['bucket_path_a'])
struct_value_2 = str(os.environ['bucket_path_b'])
struct_value_3 = str(os.environ['bucket_path_c'])
struct_value_4 = str(os.environ['bucket_path_d'])


bucket_structure = [
    struct_value_1,
    struct_value_2,
    struct_value_3,
    struct_value_4,

]

# Initiate Amazon S3 Bucket Resource
s3 = boto3.resource('s3', region_name=my_region)

def put_s3_object(my_bucket, my_key):
    try:
      bucket = s3.Bucket(my_bucket)
      my_object = bucket.Object(my_key)
      put_obj_response = my_object.put()
    except ServicesClientError as e:
      logger.error(e)
      raise


def bucket_put_event_notification(s3Bucket, my_event_one_fn_arn, my_event_two_fn_arn, my_event_three_fn_arn, my_event_four_fn_arn):
  # Initiate Bucket Notification ##
  bucket_notification = s3.BucketNotification(s3Bucket)
  try:
      put_notification_response = bucket_notification.put(
          NotificationConfiguration={
              'LambdaFunctionConfigurations': [
                  {
                      'Id': my_event_one_id,
                      'LambdaFunctionArn': my_event_one_fn_arn,
                      'Events': [
                          's3:ObjectCreated:*',
                      ],
                      'Filter': {
                          'Key': {
                              'FilterRules': [
                                  {
                                      'Name': 'prefix',
                                      'Value': my_event_one_prefix_value
                                  },
                                  {
                                      'Name': 'suffix',
                                      'Value': my_event_one_suffix_value
                                  }
                              ]
                          }
                      }
                  },
                  {
                      'Id': my_event_two_id,
                      'LambdaFunctionArn': my_event_two_fn_arn,
                      'Events': [
                          's3:ObjectCreated:*',
                      ],
                      'Filter': {
                          'Key': {
                              'FilterRules': [
                                  {
                                      'Name': 'prefix',
                                      'Value': my_event_two_prefix_value
                                  },
                                  {
                                      'Name': 'suffix',
                                      'Value': my_event_two_suffix_value
                                  }
                              ]
                          }
                      }
                  },
                  {
                      'Id': my_event_three_id,
                      'LambdaFunctionArn': my_event_three_fn_arn,
                      'Events': [
                          's3:ObjectCreated:*',
                      ],
                      'Filter': {
                          'Key': {
                              'FilterRules': [
                                  {
                                      'Name': 'prefix',
                                      'Value': my_event_three_prefix_value
                                  },
                                  {
                                      'Name': 'suffix',
                                      'Value': my_event_three_suffix_value
                                  }
                              ]
                          }
                      }
                  },
                  {
                      'Id': my_event_four_id,
                      'LambdaFunctionArn': my_event_four_fn_arn,
                      'Events': [
                          's3:ObjectCreated:*',
                      ],
                      'Filter': {
                          'Key': {
                              'FilterRules': [
                                  {
                                      'Name': 'prefix',
                                      'Value': my_event_four_prefix_value
                                  },
                                  {
                                      'Name': 'suffix',
                                      'Value': my_event_four_suffix_value
                                  }
                              ]
                          }
                      }
                  },
              ],
          },
          SkipDestinationValidation=True
      )
  except ServicesClientError as e:
      logger.error(e)
      raise


def remove_bucket_notification(s3Bucket):
  # Initiate Bucket Notification ##
  bucket_notification = s3.BucketNotification(s3Bucket)
  try:
      remove_notification_response = bucket_notification.put(
          NotificationConfiguration={}
      )
  except ServicesClientError as e:
      logger.error(e)
      raise



def lambda_handler(event, context):
  # Define Environmental Variables

  my_bucket = event.get("ResourceProperties").get("my_solution_bucket")
  my_event_one_fn_arn_value = event.get("ResourceProperties").get("bucket_event_destination_lambda")
  my_event_two_fn_arn_value = event.get("ResourceProperties").get("bucket_event_destination_lambda_1")
  my_event_three_fn_arn_value = event.get("ResourceProperties").get("bucket_event_destination_lambda")
  my_event_four_fn_arn_value = event.get("ResourceProperties").get("bucket_event_destination_lambda_state_function")

  if event.get('RequestType') == 'Create':
    logger.info(event)
    try:
      logger.info("Creating Prefixes for Bucket Structure...")
      create_object = [put_s3_object(my_bucket, r) for r in bucket_structure]
      logger.info("Initiating Bucket Notification Configuration Setting...")
      bucket_put_event_notification(my_bucket, my_event_one_fn_arn_value, my_event_two_fn_arn_value, my_event_three_fn_arn_value, my_event_four_fn_arn_value)
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
      logger.info(f"Initiating Bucket Notification Configuration Removal")
      remove_bucket_notification(my_bucket)
      responseData = {}
      responseData['message'] = "Completed"
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
    logger.info(event)
    try:
      logger.info("Initiating Bucket Notification Configuration Update")
      bucket_put_event_notification(my_bucket, my_event_one_fn_arn_value, my_event_two_fn_arn_value, my_event_three_fn_arn_value, my_event_four_fn_arn_value)
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
  else:
    logging.error(f"Unsupported Operation {event.get('RequestType')}, please retry")
