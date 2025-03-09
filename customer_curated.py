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

# Script generated for node customer_trusted
customer_trusted_node1741197520805 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project", table_name="customer_trusted", transformation_ctx="customer_trusted_node1741197520805")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1741197504978 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1741197504978")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct t1.customername, t1.email,  t1.phone, t1.birthday, t1.serialnumber, t1.registrationdate, t1.lastupdatedate, t1.sharewithresearchasofdate, t1.sharewithpublicasofdate, t1.sharewithfriendsasofdate
from customer_trusted t1 
inner join accelerometer_trusted t2
on t1.email == t2.user;
'''
SQLQuery_node1741197557522 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":customer_trusted_node1741197520805, "accelerometer_trusted":accelerometer_trusted_node1741197504978}, transformation_ctx = "SQLQuery_node1741197557522")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1741197860114 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1741197557522, database="udacity_project", table_name="customer_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1741197860114")

job.commit()
