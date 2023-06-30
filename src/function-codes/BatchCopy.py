import boto3
import os
from urllib import parse
from botocore.client import Config
from botocore.exceptions import ClientError as S3ClientError
from boto3.s3.transfer import TransferConfig
import logging
import datetime

# Define Environmental Variables
target_bucket = str(os.environ['destination_bucket'])
my_max_pool_connections = int(os.environ['max_pool_connections'])
my_max_concurrency = int(os.environ['max_concurrency'])
my_multipart_chunksize = int(os.environ['multipart_chunksize'])
my_max_attempts = int(os.environ['max_attempts'])
metadata_copy = str(os.environ['copy_metadata'])
tagging_copy = str(os.environ['copy_tagging'])
obj_copy_storage_class = str(os.environ['copy_storage_class'])
new_prefix = str(os.environ['destination_bucket_prefix'])
# my_source_storage_class = str(os.environ['source_storage_class'])
my_source_storage_class = ['GLACIER', 'DEEP_ARCHIVE']


# # Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Enable Verbose logging for Troubleshooting
# boto3.set_stream_logger("")

# Set and Declare Configuration Parameters
transfer_config = TransferConfig(max_concurrency=my_max_concurrency, multipart_chunksize=my_multipart_chunksize)
config = Config(max_pool_connections=my_max_pool_connections, retries = {'max_attempts': my_max_attempts})

# Set and Declare Copy Arguments
myargs = {'ACL': 'bucket-owner-full-control', 'StorageClass': obj_copy_storage_class}

# Instantiate S3Client
s3Client = boto3.client('s3', config=config)

def lambda_handler(event, context):
  # Parse job parameters from Amazon S3 batch operations
  jobId = event['job']['id']
  invocationId = event['invocationId']
  invocationSchemaVersion = event['invocationSchemaVersion']

  # Prepare results
  results = []

  # Parse Amazon S3 Key, Key Version, and Bucket ARN
  taskId = event['tasks'][0]['taskId']
  # use unquote_plus to handle various characters in S3 Key name
  s3Key = parse.unquote_plus(event['tasks'][0]['s3Key'], encoding='utf-8')
  s3VersionId = event['tasks'][0]['s3VersionId']
  s3BucketArn = event['tasks'][0]['s3BucketArn']
  s3Bucket = s3BucketArn.split(':')[-1]

  try:
    # Prepare result code and string
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
    else:
      # Construct/Retrieve get source key metadata
      if metadata_copy == 'Enable':
        get_metadata = s3Client.head_object(Bucket=s3Bucket, Key=s3Key)
      # Construct/Retrieve get source key tagging
      if tagging_copy == 'Enable':
        get_obj_tag = s3Client.get_object_tagging(Bucket=s3Bucket, Key=s3Key)

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
      tagging_to_s3 = "&".join([f"{parse.quote_plus(d['Key'])}={parse.quote_plus(d['Value'])}" for d in existing_tag_set])
      # Construct Request With Required and Available Arguments
      if existing_tag_set:
        myargs['Tagging'] = tagging_to_s3
    else:
      logger.info("Object Tagging Copy Disabled")

    # Include Copy Source Storage Class Condition:
    response = {}
    head_object_storage_class = get_metadata.get('StorageClass')
    logger.info(f"Storage class of source key is {head_object_storage_class}!")
    # my_source_storage_class
    if head_object_storage_class in my_source_storage_class:
      # Initiate the Actual Copy Operation and include transfer config option
      logger.info(f"starting copy of object {s3Key} with versionID {s3VersionId} between SOURCEBUCKET: {s3Bucket} and DESTINATIONBUCKET: {newBucket}")
      response = s3Client.copy(copy_source, newBucket, newKey, Config=transfer_config, ExtraArgs=myargs)
      # Confirm copy was successful
      logger.info("Successfully completed the copy process!")

      # Mark as succeeded
      resultCode = 'Succeeded'
      resultString = str("Successfully completed the copy process!")

    else:
      logger.info(f"Skipping Copy, object {s3Key} with versionID {s3VersionId} in storage-class {head_object_storage_class} is not in specified storage class {my_source_storage_class}!")
      # Mark as succeeded
      resultCode = 'PermanentFailure'
      resultString = str("Skipping copy, object storage class does not match the specified copy source storage class")

  except S3ClientError as e:
    # log errors, some errors does not have a response, so handle them
    logger.error(f"Unable to complete requested operation, see Clienterror details below:")
    try:
      logger.error(e.response)
      errorCode = e.response.get('Error', {}).get('Code')
      errorMessage = e.response.get('Error', {}).get('Message')
      errorS3RequestID = e.response.get('ResponseMetadata', {}).get('RequestId')
      errorS3ExtendedRequestID = e.response.get('ResponseMetadata', {}).get('HostId')

      resultCode = 'PermanentFailure'
      resultString = '{}: {}: {}: {}'.format(errorCode, errorMessage, errorS3RequestID, errorS3ExtendedRequestID)
      logger.error(resultString)

    except AttributeError:
      logger.error(e)
      resultCode = 'PermanentFailure'
      # Remove line feed or carriage return for compatibility with S3 Batch Result Message
      resultString = '{}'.format(str(e).translate(mycompat))
  except Exception as e:
    # log errors, some errors does not have a response, so handle them
    logger.error(f"Unable to complete requested operation, see Additional Client/Service error details below:")
    try:
      logger.error(e.response)
      errorCode = e.response.get('Error', {}).get('Code')
      errorMessage = e.response.get('Error', {}).get('Message')
      errorS3RequestID = e.response.get('ResponseMetadata', {}).get('RequestId')
      errorS3ExtendedRequestID = e.response.get('ResponseMetadata', {}).get('HostId')
      resultString = '{}: {}: {}: {}'.format(errorCode, errorMessage, errorS3RequestID, errorS3ExtendedRequestID)
    except AttributeError:
      logger.error(e)
      resultString = 'Exception: {}'.format(str(e).translate(mycompat))
    resultCode = 'PermanentFailure'

  finally:
    results.append({
    'taskId': taskId,
    'resultCode': resultCode,
    'resultString': resultString
    })

  return {
  'invocationSchemaVersion': invocationSchemaVersion,
  'treatMissingKeysAs': 'PermanentFailure',
  'invocationId': invocationId,
  'results': results
  }
