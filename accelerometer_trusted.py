import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node accelerometer_landing
accelerometer_landing_node1741108087449 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1741108087449")

# Script generated for node customer_trusted
customer_trusted_node1741106827359 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project", table_name="customer_trusted", transformation_ctx="customer_trusted_node1741106827359")

# Script generated for node SQL Query
SqlQuery0 = '''
select t2.user, t2.timestamp, t2.x, t2.y, t2.z 
from customer_trusted t1
inner join  accelerometer_landing t2
on t2.user = t1.email ;
'''
SQLQuery_node1741106968342 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":customer_trusted_node1741106827359, "accelerometer_landing":accelerometer_landing_node1741108087449}, transformation_ctx = "SQLQuery_node1741106968342")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1741196123936 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1741106968342, database="udacity_project", table_name="accelerometer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1741196123936")

job.commit()
