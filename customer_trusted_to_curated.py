import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer_trusted
Customer_trusted_node1745996454027 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="Customer_trusted_node1745996454027")

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1745996455836 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Accelerometer_trusted_node1745996455836")

# Script generated for node Join
Join_node1745996512365 = Join.apply(frame1=Accelerometer_trusted_node1745996455836, frame2=Customer_trusted_node1745996454027, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1745996512365")

# Script generated for node SQL Query
SqlQuery2481 = '''
select distinct customername,
  email,
  phone,
  birthday,
  serialnumber,
  registrationdate,
  lastupdatedate,
  sharewithresearchasofdate,
  sharewithfriendsasofdate,
  sharewithpublicasofdate from myDataSource

'''
SQLQuery_node1745996557963 = sparkSqlQuery(glueContext, query = SqlQuery2481, mapping = {"myDataSource":Join_node1745996512365}, transformation_ctx = "SQLQuery_node1745996557963")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745996557963, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745994993329", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745996731332 = glueContext.getSink(path="s3://datalakeshore/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745996731332")
AmazonS3_node1745996731332.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1745996731332.setFormat("json")
AmazonS3_node1745996731332.writeFrame(SQLQuery_node1745996557963)
job.commit()