# referenced from : https://stackoverflow.com/questions/52026405/how-to-create-dataframe-from-aws-athena-using-boto3-get-query-results-method
import time
import boto3
import pandas as pd
import io

import logging

default_log_level = logging.INFO
logging.basicConfig(format='[%(levelname)s] %(message)s', level=default_log_level)
logger = logging.getLogger(__name__)
logger.setLevel(default_log_level)


class QueryAthena:
    def __init__(self, query, database, query_output_folder, query_result_bucket, region_name):
        self.database = database
        self.bucket = query_result_bucket
        self.folder = query_output_folder
        self.s3_output = 's3://' + self.bucket + '/' + self.folder
        self.region_name = region_name
        self.query = query

    def load_conf(self, q):
        try:
            self.athena_client = boto3.client('athena', region_name=self.region_name)
            response = self.athena_client.start_query_execution(
                QueryString=q,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.s3_output}
            )
            self.filename = response['QueryExecutionId']
            logger.info('Execution ID: ' + response['QueryExecutionId'])
        except Exception as e:
            logger.error(e)
        return response

    def run_query(self):
        queries = [self.query]
        for q in queries:
            res = self.load_conf(q)
        try:
            query_status = None
            while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                query_status = \
                self.athena_client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution'][
                    'Status']['State']
                logger.info(query_status)
                if query_status == 'FAILED' or query_status == 'CANCELLED':
                    raise Exception('Athena query with the string "{}" failed or was cancelled'.format(self.query))
                time.sleep(10)
            logger.info('Query "{}" finished.'.format(self.query))

            df = self.obtain_data()
            return df
        except Exception as e:
            logger.error(e)

    def obtain_data(self):
        try:
            self.resource = boto3.resource('s3',
                                           region_name=self.region_name)
            response = self.resource.Bucket(self.bucket).Object(key=self.folder + self.filename + '.csv').get()
            return pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')
        except Exception as e:
            logger.error(e)


if __name__ == "__main__":
    '''query_result_bucket : This is bucket where we store Athena results (Need to filled if blanked)
        Amazon Athena >> Query editor >> Settings >> Query result location'''
    query_result_bucket = "query_result_bucket"
    '''query_output_folder : folder name inside query_result_bucket where you try to store api results'''
    query_output_folder = "query_output_folder"
    database = 'database'  # database name where the Athena table is located
    table_name = "table_name"  # As per requirement
    query = f"""SELECT * FROM "{database}"."{table_name}";"""
    region_name = "region_name"  # AWS region where you have the Athena tables
    qa = QueryAthena(query=query, database=database, query_output_folder=query_output_folder,
                     query_result_bucket=query_result_bucket, region_name=region_name)
    dataframe = qa.run_query()
    logger.info(dataframe)
    logger.info({f"dataframe length : {len(dataframe)}"})
