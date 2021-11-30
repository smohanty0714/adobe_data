import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

#Import pyspark modules
from pyspark.context import SparkContext
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Window
from urllib.parse import urlparse
from urllib.parse import parse_qs
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import time


#UDF

def get_domain_from_url(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc
def get_query_from_url(url):
    parsed_url = urlparse(url)
    query_val = parse_qs(parsed_url.query)['q'][0] if 'q' in parse_qs(parsed_url.query) else ''
    if query_val == '':
        query_val = parse_qs(parsed_url.query)['p'][0] if 'p' in parse_qs(parsed_url.query) else ''
    return query_val
def calculate_revenue(column):
    product_list = str(column).split('|')
    revenue = 0
    for product in product_list:
        product_attr = product.split(';')
        if len(product_attr) >= 4 and product_attr[2] != '' and product_attr[3] != '':
            revenue += (int(product_attr[2])*int(product_attr[3]))
    return revenue
    
extract_domain_udf = udf(get_domain_from_url, StringType())
extract_url_udf = udf(get_query_from_url, StringType())
extract_revenue_udf = udf(calculate_revenue, StringType())

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_BUCKET_NAME = 'adobe-data-enginneering'
date_time_prefix = time.strftime("%Y%m%d-%H%M%S")
S3_PREFIX = 'data/output/' + str(date_time_prefix) + '/'

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "adobe_db", table_name = "input", transformation_ctx = "datasource0")

#convert to Spark DF
df = datasource0.toDF() 

df = df.withColumn('domain', extract_domain_udf(df['referrer'])).withColumn('query', extract_url_udf(df['referrer']))
df = df.withColumn('revenue', extract_revenue_udf(df['product_list']))

w=Window().partitionBy("ip").orderBy("date_time")
df_rank = df.withColumn("rank", f.rank().over(w))

df_revenue = df_rank.filter('event_list == 1')
df_referrer_domain = df_rank.filter('rank == 1')

# Obtain columns lists
left_cols = df_revenue.columns
right_cols = df_referrer_domain.columns

df_revenue = df_revenue.selectExpr([col + ' as r_' + col for col in left_cols])
df_referrer_domain = df_referrer_domain.selectExpr([col + ' as l_' + col for col in right_cols])

df_result = df_referrer_domain.join(df_revenue,df_referrer_domain.l_ip ==  df_revenue.r_ip,"left")\
        .select(['l_domain', 'l_query', 'r_revenue'])\
        .fillna(value=0)\
        .orderBy(col("r_revenue").desc())
        
df_result = df_result.withColumnRenamed("l_domain","Search Engine Domain")\
    .withColumnRenamed("l_query","Search Keyword")\
    .withColumnRenamed("r_revenue","Revenue")

#Create just 1 partition, because there is so little data
data_frame_aggregated = df_result.repartition(1)

data_frame_aggregated.show(10)

data_frame_aggregated_dyf = DynamicFrame.fromDF(data_frame_aggregated, glueContext , "data_frame_aggregated_dyf")

S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=data_frame_aggregated_dyf,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://" + S3_BUCKET_NAME + "/" + S3_PREFIX,
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()

#rename the file 
import boto3
client = boto3.client('s3')

response = client.list_objects(
    Bucket=S3_BUCKET_NAME,
    Prefix=S3_PREFIX
)

name = response["Contents"][0]["Key"]
new_name = str(time.strftime("%Y-%m-%d")) + '_SearchKeywordPerformance.csv'
client.copy_object(Bucket=S3_BUCKET_NAME, CopySource=S3_BUCKET_NAME+'/'+name, Key=S3_PREFIX + new_name)
client.delete_object(Bucket=S3_BUCKET_NAME, Key=name)

