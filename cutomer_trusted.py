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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1741105228368 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1741105228368")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
Where shareWithResearchAsOfDate  is not null;
'''
SQLQuery_node1741105243948 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node1741105228368}, transformation_ctx = "SQLQuery_node1741105243948")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1741191577831 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1741105243948, database="udacity_project", table_name="customer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1741191577831")

job.commit()
