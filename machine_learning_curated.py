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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1745998231529 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1745998231529")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1745998231093 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="ster_trainer_trusted", transformation_ctx="step_trainer_trusted_node1745998231093")

# Script generated for node SQL Query
SqlQuery2601 = '''
SELECT
  st.sensorReadingTime,
  st.serialNumber,
  st.distanceFromObject,
  ac.user,
  ac.x,
  ac.y,
  ac.z,
  ac.timestamp
FROM st
JOIN ac
  ON st.sensorReadingTime = ac.timestamp
'''
SQLQuery_node1745998286870 = sparkSqlQuery(glueContext, query = SqlQuery2601, mapping = {"st":step_trainer_trusted_node1745998231093, "ac":accelerometer_trusted_node1745998231529}, transformation_ctx = "SQLQuery_node1745998286870")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745998286870, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745997437943", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745998383199 = glueContext.getSink(path="s3://datalakeshore/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745998383199")
AmazonS3_node1745998383199.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1745998383199.setFormat("json")
AmazonS3_node1745998383199.writeFrame(SQLQuery_node1745998286870)
job.commit()