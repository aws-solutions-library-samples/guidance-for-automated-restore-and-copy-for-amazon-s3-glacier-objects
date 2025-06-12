import json
import boto3
import os
from urllib import parse
from botocore.client import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import logging
import datetime
import uuid
import hashlib

# Define Environmental Variables
target_bucket = str(os.environ['destination_bucket'])
my_max_pool_connections = int(os.environ['copy_throughput'])
my_max_concurrency = int(os.environ['copy_throughput'])
my_multipart_chunksize = int(os.environ['multipart_chunksize'])
my_multipart_threshold = int(os.environ['multipart_threshold'])
my_max_attempts = int(os.environ['max_attempts'])
metadata_copy = str(os.environ['copy_metadata'])
tagging_copy = str(os.environ['copy_tagging'])
obj_copy_storage_class = str(os.environ['copy_storage_class'])
new_prefix = str(os.environ['destination_bucket_prefix'])
my_source_storage_class = ['GLACIER', 'DEEP_ARCHIVE']
# For DynamoDB
my_region = str(os.environ['AWS_REGION'])
my_destination_bucket = str(os.environ['destination_bucket'])
my_dynamodb_name = str(os.environ['dynamodb_name'])

# # Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Enable Verbose logging for Troubleshooting
# boto3.set_stream_logger("")

# Set and Declare Configuration Parameters
transfer_config = TransferConfig(max_concurrency=my_max_concurrency, multipart_chunksize=my_multipart_chunksize,
                                multipart_threshold=my_multipart_threshold)
config = Config(max_pool_connections=my_max_pool_connections, retries={'max_attempts': my_max_attempts})

# Set and Declare Copy Arguments
myargs = {'ACL': 'bucket-owner-full-control', 'StorageClass': obj_copy_storage_class}

# Instantiate S3Client
s3Client = boto3.client('s3', config=config)
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



# Initiate Main function
# Retry failed invocations with Report Batch Items Failure

def lambda_handler(event, context):
    if event:
        batch_item_failures = []
        sqs_batch_response = {}
    
        for record in event["Records"]:
            try:
                # process message, and initiate S3 copy object
                message_body = json.loads(record.get('body'))
                # Specify Parameters
                s3Bucket = str(message_body.get('detail').get('bucket').get('name'))
                s3Key = parse.unquote_plus(message_body.get('detail').get('object').get('key'), encoding='utf-8')
                s3VersionId = message_body.get('detail').get('object').get('version-id')     
                source_bucket_region = str(message_body.get('region'))
                obj_url = None                                               

                # Set DDB Item Expiration to 30 days #
                num_days = 30
                service_ddb_item_expiration = int((datetime.datetime.now() + datetime.timedelta(num_days)).timestamp())
                
                # Include timestamp in the DDB entry:
                time_completed = str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M"))

                # Prepare results
                results = []

                try:
                    resultCode = None
                    resultString = None
                    # Remove line feed or carriage return for compatibility with S3 Batch Result Message
                    # Will use str.translate to strip '\n' and '\r'. Convert both char to ascii using ord()
                    # where '\t' = 9, '\n' = 10 and '\r' = 13
                    mycompat = {9: None, 10: None, 13: None}
                    # Construct Copy Object
                    copy_source = {'Bucket': s3Bucket, 'Key': s3Key}
                    # If source key has VersionID, then construct request with VersionID
                    if s3VersionId is not None:
                        copy_source['VersionId'] = s3VersionId
                        # Construct/Retrieve get source key metadata
                        if metadata_copy == 'Enable':
                            get_metadata = s3Client.head_object(Bucket=s3Bucket, Key=s3Key, VersionId=s3VersionId)
                        # Construct/Retrieve get source key tagging
                        if tagging_copy == 'Enable':
                            get_obj_tag = s3Client.get_object_tagging(Bucket=s3Bucket, Key=s3Key, VersionId=s3VersionId)
                        # Set Object URL for DDB
                        sourceKeyVersionID = s3VersionId
                        obj_url = f"https://{s3Bucket}.s3.{source_bucket_region}.amazonaws.com/{s3Key}?versionId={sourceKeyVersionID}"                                      
                    else:
                        # Construct/Retrieve get source key metadata
                        if metadata_copy == 'Enable':
                            get_metadata = s3Client.head_object(Bucket=s3Bucket, Key=s3Key)
                        # Construct/Retrieve get source key tagging
                        if tagging_copy == 'Enable':
                            get_obj_tag = s3Client.get_object_tagging(Bucket=s3Bucket, Key=s3Key)
                        # Set Object URL hash with no versionID
                        sourceKeyVersionID = 'null'
                        obj_url = f"https://{s3Bucket}.s3.{source_bucket_region}.amazonaws.com/{s3Key}"    

                    # Get object URL hash
                    obj_url_hash = str(hashlib.md5(obj_url.encode(encoding='UTF-8'), usedforsecurity=False).hexdigest())                                                                    

                    # Construct New Path
                    # Construct New Key
                    if new_prefix and len(new_prefix) > 0:
                        newKey = "{0}/{1}".format(new_prefix, s3Key)
                    else:
                        newKey = s3Key

                    newBucket = target_bucket

                    # Toggle Metadata or Tagging Copy Based on Enviromental Variables
                    # Construct Request Parameters with metadata and tagging from sourceKey
                    # Create variables to append as metadata and tagging to destination object
                    if metadata_copy == 'Enable':
                        logger.info("Object Metadata Copy Enabled from Source to Destination")
                        cache_control = get_metadata.get('CacheControl')
                        content_disposition = get_metadata.get('ContentDisposition')
                        content_encoding = get_metadata.get('ContentEncoding')
                        content_language = get_metadata.get('ContentLanguage')
                        metadata = get_metadata.get('Metadata')
                        website_redirect_location = get_metadata.get('WebsiteRedirectLocation')
                        expires = get_metadata.get('Expires')
                        # Construct Request With Required and Available Arguments
                        if cache_control:
                            myargs['CacheControl'] = cache_control
                        if content_disposition:
                            myargs['ContentDisposition'] = content_disposition
                        if content_encoding:
                            myargs['ContentEncoding'] = content_encoding
                        if content_language:
                            myargs['ContentLanguage'] = content_language
                        if metadata:
                            myargs['Metadata'] = metadata
                        if website_redirect_location:
                            myargs['WebsiteRedirectLocation'] = website_redirect_location
                        if expires:
                            myargs['Expires'] = expires
                    else:
                        logger.info("Object Metadata Copy Disabled")

                    if tagging_copy == 'Enable':
                        logger.info("Object Tagging Copy Enabled from Source to Destination")
                        existing_tag_set = (get_obj_tag.get('TagSet'))
                        # Convert the Output from get object tagging to be compatible with transfer s3.copy()
                        tagging_to_s3 = "&".join \
                            ([f"{parse.quote_plus(d['Key'])}={parse.quote_plus(d['Value'])}" for d in existing_tag_set])
                        # Construct Request With Required and Available Arguments
                        if existing_tag_set:
                            myargs['Tagging'] = tagging_to_s3
                    else:
                        logger.info("Object Tagging Copy Disabled")

                    # Start the copy process:
                    # Initiate the Actual Copy Operation and include transfer config option
                    # First check the object is in Glacier or Deep Archive
                    head_object_storage_class = get_metadata.get('StorageClass')
                    logger.info(f"Storage class of source key is {head_object_storage_class}!")
                    if head_object_storage_class in my_source_storage_class:
                        # Initiate the Actual Copy Operation and include transfer config option
                        logger.info \
                            (f"starting copy of object {s3Key} with versionID {s3VersionId} between SOURCEBUCKET: {s3Bucket} and DESTINATIONBUCKET: {newBucket}")  
                        response = s3Client.copy(copy_source, newBucket, newKey, Config=transfer_config, ExtraArgs=myargs)
                        # Confirm copy was successful
                        logger.info("Successfully completed the copy process!")
                        # Mark as succeeded
                        resultCode = 'Succeeded'
                        resultString = str(response)

                    ## Commence updating DDB with Successful Response
                    # Generate input for DDB
                    my_formatted_data = {
                        'obj_url_hash': obj_url_hash,
                        'bucket_name': s3Bucket,
                        'obj_key': s3Key,
                        'obj_ver': sourceKeyVersionID,
                        's3_key_copy_status': 'Successful',
                        's3_key_destination_bucket': my_destination_bucket,
                        's3_key_restore_status': 'Restore Completed',
                        's3_service_client_error': resultString,
                        'service_ddb_item_expiration': service_ddb_item_expiration,
                        'time_completed': time_completed
                    }

                    logger.info(f"Initiate PutItem to DynamoDB table:.....")
                    ddb_entry(my_formatted_data)

                # Catch All Errors including From the SDK and Service, do not raise but send to DynamoDB
                except ClientError as e:
                    resultCode = 'PermanentFailure'
                    # log errors, some errors does not have a response, so handle them
                    logger.error(f"Unable to complete requested operation, see Clienterror details below:")
                    try:
                        logger.error(e)
                        logger.error(e.response)
                        errorCode = e.response.get('Error', {}).get('Code')
                        errorMessage = e.response.get('Error', {}).get('Message')
                        errorS3RequestID = e.response.get('ResponseMetadata', {}).get('RequestId')
                        errorS3ExtendedRequestID = e.response.get('ResponseMetadata', {}).get('HostId')
                        resultString = '{}: {}: {}: {}'.format(errorCode, errorMessage, errorS3RequestID, errorS3ExtendedRequestID)

                    
                    
                    except AttributeError:
                        logger.error(e)
                        resultString = 'Exception: {}'.format(str(e).translate(mycompat))
                    
                    
                    # Insert failure report into the DynamoDB
                    # Commence updating DDB with Successful Response
                    # Generate input for DDB
                    my_formatted_data = {
                      'obj_url_hash': obj_url_hash,
                      'bucket_name': s3Bucket,
                      'obj_key': s3Key,
                      'obj_ver': sourceKeyVersionID,
                      's3_key_copy_status': 'Failed',
                      's3_key_destination_bucket': my_destination_bucket,
                      's3_key_restore_status': 'Restore Completed',
                      's3_service_client_error': resultString,
                      'service_ddb_item_expiration': service_ddb_item_expiration,
                      'time_completed': time_completed
                    }
                    # Store error details in DynamoDB
                    logger.info(f"Initiate PutItem to DynamoDB table:.....")
                    ddb_entry(my_formatted_data)                               
                
                # Catch All other Errors raise and send back to the SQS queue for retry
                except Exception as e:
                    resultCode = 'PermanentFailure'
                    # log errors, some errors does not have a response, so handle them
                    logger.error(f"Unable to complete requested operation, see Client/Service error details below:")
                    try:
                        logger.error(e)
                        logger.error(e.response)
                        errorCode = e.response.get('Error', {}).get('Code')
                        errorMessage = e.response.get('Error', {}).get('Message')
                        errorS3RequestID = e.response.get('ResponseMetadata', {}).get('RequestId')
                        errorS3ExtendedRequestID = e.response.get('ResponseMetadata', {}).get('HostId')
                        resultString = '{}: {}: {}: {}'.format(errorCode, errorMessage, errorS3RequestID,
                                                              errorS3ExtendedRequestID)
                        raise Exception(resultString)

                    except AttributeError:
                        logger.error(e)
                        resultString = 'Exception: {}'.format(str(e).translate(mycompat))
                        raise Exception(resultString)

            # Finally send Exception errors that were raised back to SQS queue
            except Exception as e:
                logger.error(e)
                batch_item_failures.append({"itemIdentifier": record['messageId']})
        
        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response
