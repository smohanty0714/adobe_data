import json
import urllib.parse
import boto3
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

print('Loading function')

s3 = boto3.client('s3')
glue_client = boto3.client('glue')
glueJobName = "adobe-data-processor-2"

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    logger.info(event['detail'])
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        logger.info('## INITIATED BY EVENT: ')
        response = glue_client.start_job_run(JobName = glueJobName)
        logger.info('## STARTED GLUE JOB: ' + glueJobName)
        logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
        return response
        
    except Exception as e:
        print(e)
        raise e
