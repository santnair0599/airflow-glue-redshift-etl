import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Script generated for node Custom Transform - add load_date
def data_load(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import current_date, to_date, col
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    # Use the correct input variable (dfc)
    input_dyf = dfc.select(list(dfc.keys())[0])

    # Convert to Spark DataFrame
    df = input_dyf.toDF()

    # Convert ingestion date to proper yyyy-mm-dd format
    df = df.withColumn("ingestion_date", to_date(col("ingestion_date"), "yyyy-MM-dd"))

    # Add load_date column
    df = df.withColumn("load_date", current_date())

    # Convert back to DynamicFrame
    updated_dyf = DynamicFrame.fromDF(df, glueContext, "UpdatedFrame")

    # Return as a DynamicFrameCollection
    return DynamicFrameCollection({"output": updated_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1743500218874 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://kaggle-dataset-019824224614/raw/"], "recurse": True}, transformation_ctx="AmazonS3_node1743500218874")

# Script generated for node Change Schema
ChangeSchema_node1743500227998 = ApplyMapping.apply(frame=AmazonS3_node1743500218874, mappings=[("customerid", "string", "customerid", "string"), ("country", "string", "country", "string"), ("state", "string", "state", "string"), ("city", "string", "city", "string"), ("zip code", "string", "zip_code", "int"), ("lat long", "string", "lat_long", "decimal"), ("latitude", "string", "latitude", "decimal"), ("longitude", "string", "longitude", "decimal"), ("gender", "string", "gender", "string"), ("senior citizen", "string", "senior_citizen", "string"), ("partner", "string", "partner", "string"), ("dependents", "string", "dependents", "string"), ("tenure months", "string", "tenure_months", "int"), ("phone service", "string", "phone_service", "string"), ("multiple lines", "string", "multiple_lines", "string"), ("internet service", "string", "internet_service", "string"), ("online security", "string", "online_security", "string"), ("online backup", "string", "online_backup", "string"), ("device protection", "string", "device_protection", "string"), ("tech support", "string", "tech_support", "string"), ("streaming tv", "string", "streaming_tv", "string"), ("streaming movies", "string", "streaming_movies", "string"), ("contract", "string", "contract", "string"), ("paperless billing", "string", "paperless_billing", "string"), ("payment method", "string", "payment_method", "string"), ("monthly charges", "string", "monthly_charges", "decimal"), ("total charges", "string", "total_charges", "decimal"), ("churn label", "string", "churn_label", "string"), ("churn value", "string", "churn_value", "int"), ("churn score", "string", "churn_score", "int"), ("cltv", "string", "cltv", "int"), ("churn reason", "string", "churn_reason", "string"), ("ingestion_date", "string", "ingestion_date", "date")], transformation_ctx="ChangeSchema_node1743500227998")

# Script generated for node Custom Transform - add load_date
CustomTransformaddload_date_node1743503694325 = data_load(glueContext, DynamicFrameCollection({"ChangeSchema_node1743500227998": ChangeSchema_node1743500227998}, glueContext))

# Script generated for node Select From Collection - collect load date
SelectFromCollectioncollectloaddate_node1743505539671 = SelectFromCollection.apply(dfc=CustomTransformaddload_date_node1743503694325, key=list(CustomTransformaddload_date_node1743503694325.keys())[0], transformation_ctx="SelectFromCollectioncollectloaddate_node1743505539671")

# Script generated for node Amazon Redshift
AmazonRedshift_node1743500646591 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollectioncollectloaddate_node1743505539671, connection_type="redshift", connection_options={"postactions": "BEGIN; DELETE FROM public.customer_churn_optimized USING public.customer_churn_optimized_temp_jzqchl WHERE public.customer_churn_optimized_temp_jzqchl.customerid = public.customer_churn_optimized.customerid; INSERT INTO public.customer_churn_optimized SELECT * FROM public.customer_churn_optimized_temp_jzqchl; DROP TABLE public.customer_churn_optimized_temp_jzqchl; END;", "redshiftTmpDir": "s3://kaggle-dataset-019824224614/temp/", "useConnectionProperties": "true", "dbtable": "public.customer_churn_optimized_temp_jzqchl", "connectionName": "Redshift-connection-provisioned", "preactions": "CREATE TABLE IF NOT EXISTS public.customer_churn_optimized (customerid VARCHAR, country VARCHAR, state VARCHAR, city VARCHAR, zip_code INTEGER, lat_long DECIMAL, latitude DECIMAL, longitude DECIMAL, gender VARCHAR, senior_citizen VARCHAR, partner VARCHAR, dependents VARCHAR, tenure_months INTEGER, phone_service VARCHAR, multiple_lines VARCHAR, internet_service VARCHAR, online_security VARCHAR, online_backup VARCHAR, device_protection VARCHAR, tech_support VARCHAR, streaming_tv VARCHAR, streaming_movies VARCHAR, contract VARCHAR, paperless_billing VARCHAR, payment_method VARCHAR, monthly_charges DECIMAL, total_charges DECIMAL, churn_label VARCHAR, churn_value INTEGER, churn_score INTEGER, cltv INTEGER, churn_reason VARCHAR, ingestion_date DATE, load_date DATE); DROP TABLE IF EXISTS public.customer_churn_optimized_temp_jzqchl; CREATE TABLE public.customer_churn_optimized_temp_jzqchl AS SELECT * FROM public.customer_churn_optimized WHERE 1=2;"}, transformation_ctx="AmazonRedshift_node1743500646591")

job.commit()