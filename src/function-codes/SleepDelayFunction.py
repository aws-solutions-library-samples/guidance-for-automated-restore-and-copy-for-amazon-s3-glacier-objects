import time
import cfnresponse


def lambda_handler(event, context):
    # Delay for Solution S3 Bucket Event Notification to be consistent
    if event.get('RequestType') == 'Create':
        # nosemgrep: arbitrary-sleep
        time.sleep(360) # nosemgrep: arbitrary-sleep
        print("Wait time of 360 seconds over, proceed to other resource deployments...")
    responseData = {}
    cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
