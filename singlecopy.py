import argparse
import json
import time
import uuid
import logging
import ntpath
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, timedelta, datetime
from logging.handlers import RotatingFileHandler
import logging

"""
Creates a rotating log
"""
logfile = logging.getLogger("Rotating Log")
logfile.setLevel(logging.INFO)
log_file = "singlecopy.log"

# add a rotating handler
handler = RotatingFileHandler(log_file, maxBytes=1000000,
                              backupCount=5)
logfile.addHandler(handler)

PROJECT_ID='nyt-dsr-prd'
DATASET_ID='nyt_singlecopy'

# [START write_log]
def write_log(logger, project_id, logtext):

    log_data = {
        "entries": [
            {
                "logName": "projects/44920874077/logs/singlecopy_log",
                "resource": {
                    "type": "gce_instance",
                    "labels": {}
                },
                "labels": {},
                "textPayload": logtext
            }
        ]
    }

    logger.entries().write(
        body=log_data).execute()
# [END write_log]

def check_if_data_exists(bigquery, num_retries=5):

    job_data = {
        "jobReference": {
            "projectId": PROJECT_ID,
            "jobId": str(uuid.uuid4())
        },
        "configuration": {
            "query": {
                "query": "SELECT * FROM `nyt-dsr-prd.nyt_singlecopy.singlecopy` WHERE create_date = (select current_DATE()) limit 1",
                #"query": "SELECT * FROM `nyt-dsr-prd.nyt_singlecopy.singlecopy` WHERE create_date = DATE('2015-06-17') limit 1",
                "useLegacySql": False,
                }
            }
    }

    job = bigquery.jobs().insert(
        projectId=PROJECT_ID,
        body=job_data).execute(num_retries=num_retries)

    request = bigquery.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    totalRows = 0
    while True:
        result = request.execute(num_retries=2)

        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            #print result
            data = bigquery.jobs().getQueryResults(
              maxResults=1,
              pageToken=None,
              # Sets the timeout to 0 because we assume the table is already ready.
              timeoutMs=0,
              projectId=PROJECT_ID,
              jobId=job['jobReference']['jobId']).execute()
            totalRows = int(data['totalRows'])
            return totalRows
            print('Job complete.')

        time.sleep(1)

    return totalRows

# [START load_table]

def load_data_from_gcs(bigquery, table_name, file_name, num_retries=5):

    job_data = {
        "jobReference": {
            "projectId": PROJECT_ID,
            "jobId": str(uuid.uuid4())
        },
        "configuration": {
            "load": {
                "destinationTable":
                {
                    'datasetId': DATASET_ID,
                    'projectId': PROJECT_ID,
                    'tableId': table_name 
                },
                "sourceUris": ['gs://nyt-singlecopy/incoming/' + file_name ],
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_TRUNCATE",
                "printHeader": False
            }
        }
    }

    return bigquery.jobs().insert(
        projectId=PROJECT_ID,
        body=job_data).execute(num_retries=num_retries)


def insert_data_singlecopy_all(bigquery,num_retries=5):
    job_data = {
        "jobReference": {
            "projectId": PROJECT_ID,
            "jobId": str(uuid.uuid4())
        },
        "configuration": {
            "query": {
                "query": "insert nyt_singlecopy.singlecopy_all (account,bipad,transaction_date,draw,reorder,copy_return,short,pullbill,store_copy,printsite,printsite_name,allot_type,company,seasonal,ds_regulate,sold,wholesaler,create_date) SELECT account,bipad,transaction_date,draw,reorder,copy_return,short,pullbill,store_copy,printsite,printsite_name,allot_type,company,seasonal,ds_regulate,(DRAW + REORDER + COPY_RETURN + SHORT + PULLBILL - STORE_COPY) as SOLD,null,CURRENT_DATE() as create_date FROM nyt_singlecopy.sbtext",
                "useLegacySql": False
            }
        }
    }

    return bigquery.jobs().insert(
        projectId=PROJECT_ID,
        body=job_data).execute(num_retries=num_retries)


def insert_data_singlecopy_bypass_all(bigquery,num_retries=5):
    job_data = {
        "jobReference": {
            "projectId": PROJECT_ID,
            "jobId": str(uuid.uuid4())
        },
        "configuration": {
            "query": {
               "query": "insert nyt_singlecopy.singlecopy_bypass_all (bypass_date,company,description,create_date) SELECT bypass_date,company,description,CURRENT_DATE() as create_date FROM nyt_singlecopy.dsstext",
                "useLegacySql": False
            }
        }
    }

    return bigquery.jobs().insert(
        projectId=PROJECT_ID,
        body=job_data).execute(num_retries=num_retries)
 
# [START poll_job]
def poll_job(bigquery, job):
    """Waits for a job to complete."""

    print('Waiting for job to finish...')

    request = bigquery.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    while True:
        result = request.execute(num_retries=2)

        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            print('Job complete.')
            return

        time.sleep(1)
# [END poll_job]

# [START run]

def file(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def main(filename, poll_interval, num_retries):

    SERVICE_ACCOUNT_EMAIL = 'nyt-dsr-aam-usage1@nyt-dsr-prd.iam.gserviceaccount.com'
    SCOPES = ['https://www.googleapis.com/auth/bigquery',
              'https://www.googleapis.com/auth/logging.write',
              'https://www.googleapis.com/auth/logging.read']

    # [START build_service]
    # Grab the application's default credentials from the environment.
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        '/home/nimbul3/aam_usage/bqkey.json', SCOPES)

    # Construct the service object for interacting with the BigQuery API, Stackdriver API.
    bigquery = build('bigquery', 'v2', credentials=credentials)
    logger = build('logging', 'v2', credentials=credentials)
    # [END build_service]

    logtext = datetime.now().strftime("%a, %d %b %Y %H:%M:%S") + \
        "Starting to process SingleCopy files... " 
    write_log(
        logger,
        PROJECT_ID,
        logtext)

    #check if singlecopy has already been processed then exit 

    totalRows=0
    totalRows = check_if_data_exists(bigquery)
  
    print "finished checking"

    if totalRows==1:
       print "data exists"
       return 
        
    print "no data so starting to process the singlecopy data"
    
    if filename.startswith("SB"):
        logtext =  "loading data into sbtext table from gcs"
	table_name='sbtext'
    elif filename.startswith("DSS"):
        logtext =  "loading data into dsstext table from gcs"
        table_name='dsstext'
    else:
        table_name = "none"
        logtext = "unknown file"

    write_log(
        logger,
        PROJECT_ID,
        logtext)


    job = load_data_from_gcs(
       bigquery,
       table_name,
       filename,
       num_retries
    )

    poll_job(bigquery, job)
 
    logtext = "finished loaded data into " + table_name + " from gcs" 
    write_log(
        logger,
        PROJECT_ID,
        logtext)
 
    if filename.startswith("SB"):
         logtext = "inserting data into singlecopy_all"
         job=insert_data_singlecopy_all(bigquery)
    elif filename.startswith("DSS"):
        logtext = "inserting data into singlecopy_bypass_all"
        job=insert_data_singlecopy_bypass_all(bigquery)
    else: 
        logtext = "unknown"
     
    poll_job(bigquery, job) 

    logtext = "finished inserting data into master tables"
    write_log(
        logger,
        PROJECT_ID,
        logtext)


    logtext = "Processing of Singlecopy files completed"
    write_log(
        logger,
        PROJECT_ID,
        logtext)

    print "end main"
# [END run]


# [START main]
if __name__ == '__main__':


    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'file_name', help='Name of the file in google storage')
    parser.add_argument(
        '-p', '--poll_interval',
        help='How often to poll the query for completion (seconds).',
        type=int,
        default=1)
    parser.add_argument(
        '-r', '--num_retries',
        help='Number of times to retry in case of 500 error.',
        type=int,
        default=5)

    args = parser.parse_args()

    logfile.info(datetime.now().strftime("%a, %d %b %Y %H:%M:%S") +
               " Processing singlecopy file  : " + args.file_name)

    main(
        file(args.file_name),
        args.poll_interval,
        args.num_retries)
# [END main]
