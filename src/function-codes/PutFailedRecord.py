import os
import boto3
import datetime
import json
import logging
import uuid
import hashlib
from urllib import parse
from botocore.exceptions import ClientError

# Enable debugging for troubleshooting
# boto3.set_stream_logger("")


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])
my_destination_bucket = str(os.environ['destination_bucket'])
my_dynamodb_name = str(os.environ['dynamodb_name'])



# Initiate Service Clients #
dynamodb = boto3.resource('dynamodb', region_name=my_region)
table = dynamodb.Table(my_dynamodb_name)


# Amazon DynamoDB Function

def ddb_entry(data):
  print(f"Initiating PutItem Session")
  try:
      response = table.put_item(
          Item=data,
      )
  except ClientError as e:
      logger.error(e)
      raise 
  except Exception as e:
      logger.error(e)
      raise e
  else:
      logger.info(f"Successfully Added Records to the DynamoDB Table")
      return response


def lambda_handler(event, context):
    logger.info(f'Event details: {event}')

    if event:
        batch_item_failures = []
        sqs_batch_response = {}
    
        for record in event["Records"]:
            try:
              # process message, and initiate S3 copy object
              logger.info(f'Converting the stringified message body to JSON using json.loads')
              message_body = json.loads(record.get('body'))

              ## Define variables ##
              source_bucket = str(message_body.get('detail').get('bucket').get('name'))
              sourceKey = parse.unquote_plus(message_body.get('detail').get('object').get('key'), encoding='utf-8')
              VersionID = message_body.get('detail').get('object').get('version-id')     
              source_bucket_region = str(message_body.get('region'))

              # Set DDB Item Expiration to 30 days #
              num_days = 30
              service_ddb_item_expiration = int((datetime.datetime.now() + datetime.timedelta(num_days)).timestamp())   
              
              # Include timestamp in the DDB entry:
              time_completed = str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M"))              

              # Define Object URL Hash
              obj_url = None
              if VersionID is not None:
                  sourceKeyVersionID = VersionID
                  obj_url = f"https://{source_bucket}.s3.{source_bucket_region}.amazonaws.com/{sourceKey}?versionId={sourceKeyVersionID}"
              else:
                  sourceKeyVersionID = 'null'
                  obj_url = f"https://{source_bucket}.s3.{source_bucket_region}.amazonaws.com/{sourceKey}"     

              # Get object URL hash
              obj_url_hash = str(hashlib.md5(obj_url.encode(encoding='UTF-8'), usedforsecurity=False).hexdigest())                                                   

                
              # Define Result String
              resultString = "Other Exceptions Including Function Timeout"                                           

              # Prepare formatted data for DynamoDB                      
              my_formatted_data = {
                  'obj_url_hash': obj_url_hash,
                  'bucket_name': source_bucket,
                  'obj_key': sourceKey,
                  'obj_ver': sourceKeyVersionID,
                  's3_key_copy_status': 'Failed',
                  's3_key_destination_bucket': my_destination_bucket,
                  's3_key_restore_status': 'Restore Completed',
                  's3_service_client_error': resultString,
                  'service_ddb_item_expiration': service_ddb_item_expiration,
                  'time_completed': time_completed
              }    
              
              logger.info(f"Initiate PutItem to DynamoDB table:.....")
              ddb_entry(my_formatted_data)

            except Exception as e:
                logger.error(e)
                batch_item_failures.append({"itemIdentifier": record['messageId']})
      
        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response                        
