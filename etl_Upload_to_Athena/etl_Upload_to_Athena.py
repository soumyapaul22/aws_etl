import boto3
import pandas as pd
import awswrangler as wr
import logging

default_log_level = logging.INFO
logging.basicConfig(format='[%(levelname)s] %(message)s',level=default_log_level)
logger = logging.getLogger(__name__)
logger.setLevel(default_log_level)


s3_client = boto3.client('s3')


def get_fileObject(bucket_name,prefix):
    objectList = []
    try:
        response = s3_client.list_objects(
            Bucket=bucket_name,
            Prefix=prefix)
        items = response.get('Contents')
        for item in items:
            if '.csv' in item['Key']:
                objectList.append(item['Key'])
    except Exception as e:
        print(f'error : {e}')
    return(objectList)


def read_objects(bucket_name, objectname_Key):
    try:
        response = s3_client.get_object(Bucket=bucket_name,
                                        Key= objectname_Key)
        body = response.get('Body')
        df = pd.read_csv(body)
        return df
        # for json =>
        # json_string = body.read().decode('utf-8')
        # json_obj = json.loads(json_string)
    except Exception as e:
        print(f'error : {e}')
        return df


def write_to_athena(df, anthea_bucket_name, group_name):
    write_path = f"s3://{anthea_bucket_name}/aws_wragler_loading/{group_name}"
    partition_col_list = ['year','month','day']
    response = wr.s3.to_parquet(df=df, path=write_path, mode='append', dataset=True,
                                partition_cols=partition_col_list, index=False,
                                database="analytics", table=group_name)
    print(response)


if __name__ == '__main__':
    bucket_name = "dly-transaction"
    anthea_bucket_name = "athena-bucket-2024"
    group_name = "dly-transaction"
    prefix = "input_data"
    objectList = get_fileObject(bucket_name,prefix=prefix)
    print(len(objectList))
    df_final = pd.DataFrame()
    for objectname_Key in objectList:
        df = read_objects(bucket_name, objectname_Key)
        if len(df_final)!=0:
            df_final.append(df)
        else:
            df_final = df
    df_final['created_at'].fillna('01-01-1900', inplace = True) #replacing null value with default
    dateList = df_final['created_at'].unique().tolist()
    for each_date in dateList:
        print(each_date)
        delta_df = df_final[df_final['created_at']==each_date]
        delta_df['year'] = each_date.split('-')[2]
        delta_df['month'] = each_date.split('-')[1]
        delta_df['day'] = each_date.split('-')[0]
        write_to_athena(delta_df, anthea_bucket_name, group_name)

