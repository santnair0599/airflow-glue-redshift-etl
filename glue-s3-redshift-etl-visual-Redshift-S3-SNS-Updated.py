import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Script generated for node Custom Transform - add load_date
def data_load(glueContext, dfc) -> DynamicFrameCollection:
    import boto3
    import re
    from pyspark.sql.functions import current_date, to_date, col
    from pyspark.sql.types import IntegerType, DecimalType
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    # ✅ Get Spark session
    spark = glueContext.spark_session

    # ✅ SNS client (reuse throughout the function)
    sns_client = boto3.client("sns")
    sns_topic_arn = "arn:aws:sns:us-east-1:019824224614:glue-s3-redshift-customer-churn-topic"

    # ✅ Define expected schema
    expected = set([
        "customerid", "country", "state", "city", "zip_code", "lat_long", "latitude", "longitude",
        "gender", "senior_citizen", "partner", "dependents", "tenure_months", "phone_service",
        "multiple_lines", "internet_service", "online_security", "online_backup", "device_protection",
        "tech_support", "streaming_tv", "streaming_movies", "contract", "paperless_billing",
        "payment_method", "monthly_charges", "total_charges", "churn_label", "churn_value",
        "churn_score", "cltv", "churn_reason", "ingestion_date", "load_date"
    ])

    # ✅ Read input
    input_dyf = dfc.select(list(dfc.keys())[0])
    df = input_dyf.toDF()

    # ✅ Normalize column names
    original_cols = df.columns
    renamed_cols = [re.sub(r'_+$', '', re.sub(r'\W+', '_', c.strip().lower())) for c in original_cols]
    df = df.toDF(*renamed_cols)

    # 🔍 Log and notify normalized columns
    print("📥 Original Columns:", original_cols)
    print("✅ Renamed Columns:", df.columns)


    # ✅ Type Casting
    int_columns = ["zip_code", "tenure_months", "churn_value", "churn_score", "cltv"]
    for col_name in int_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(IntegerType()))

    decimal_columns = ["latitude", "longitude", "monthly_charges", "total_charges"]
    for col_name in decimal_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(DecimalType(30, 10)))

    # ✅ Parse ingestion_date
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    df = df.withColumn("ingestion_date", to_date(col("ingestion_date"), "M/d/yyyy"))
    df = df.filter(col("ingestion_date").isNotNull())

    # ✅ Add load_date
    df = df.withColumn("load_date", current_date())
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject="ℹ️ GLUE SCHEMA NORMALIZED",
        Message=f"Final Columns: {df.columns}"
    )
    # ✅ Schema validation
    actual = set(df.columns)
    missing = expected - actual
    extra = actual - expected

    print("🛑 Missing Columns:", sorted(missing))
    print("⚠️ Extra Columns:", sorted(extra))

    if missing:
        message = f"❌ Missing Columns:\n{', '.join(sorted(missing))}\n"
        if extra:
            message += f"⚠️ Extra Columns:\n{', '.join(sorted(extra))}\n"
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject="🛑 GLUE SCHEMA VALIDATION ALERT",
            Message=message
        )
        raise Exception("❌ Job Failed due to missing required columns.")

    if extra:
        message = f"⚠️ Extra Columns Present (but proceeding):\n{', '.join(sorted(extra))}\n"
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject="⚠️ GLUE SCHEMA WARNING - Extra Columns",
            Message=message
        )

    # ✅ Convert to DynamicFrame
    cleaned_dyf = DynamicFrame.fromDF(df, glueContext, "CleanedFrame")
    return DynamicFrameCollection({"output": cleaned_dyf}, glueContext)    
# Script generated for node Custom Transform - Partition
def partition_date(glueContext, dfc) -> DynamicFrameCollection:
    import boto3
    import re
    from pyspark.sql.functions import current_date, to_date, col, year, month
    from pyspark.sql.types import IntegerType, DecimalType
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    spark = glueContext.spark_session
    sns_client = boto3.client("sns")
    sns_topic_arn = "arn:aws:sns:us-east-1:019824224614:glue-s3-redshift-customer-churn-topic"



    # ✅ Correct: Select by known key (e.g. CleanedFrame)
    input_dyf = dfc.select("CleanedFrame")
    df = input_dyf.toDF()

    original_cols = df.columns
    renamed_cols = [re.sub(r'_+$', '', re.sub(r'\W+', '_', c.strip().lower())) for c in original_cols]
    df = df.toDF(*renamed_cols)

    # Cast columns
    ...

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    if "ingestion_date" in df.columns:
        df = df.withColumn("ingestion_date", to_date(col("ingestion_date"), "M/d/yyyy"))
        df = df.filter(col("ingestion_date").isNotNull())
        df = df.withColumn("year", year(col("ingestion_date")))
        df = df.withColumn("month", month(col("ingestion_date")))
    else:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject="🛑 GLUE PARTITIONING FAILED",
            Message="❌ ingestion_date column is missing. Cannot proceed with partitioning."
        )
        raise Exception("❌ ingestion_date column missing")

    df = df.withColumn("load_date", current_date())
    ...

    enriched_dyf = DynamicFrame.fromDF(df, glueContext, "EnrichedPartitionedDF")
    return DynamicFrameCollection({"output": enriched_dyf}, glueContext)
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

# Script generated for node Amazon S3
AmazonS3_node1743500218874 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://kaggle-dataset-019824224614/raw/"], "recurse": True}, transformation_ctx="AmazonS3_node1743500218874")

# Script generated for node Custom Transform - add load_date
CustomTransformaddload_date_node1743503694325 = data_load(glueContext, DynamicFrameCollection({"AmazonS3_node1743500218874": AmazonS3_node1743500218874}, glueContext))

# Script generated for node Select From Collection - collect load date
SelectFromCollectioncollectloaddate_node1743505539671 = SelectFromCollection.apply(dfc=CustomTransformaddload_date_node1743503694325, key=list(CustomTransformaddload_date_node1743503694325.keys())[0], transformation_ctx="SelectFromCollectioncollectloaddate_node1743505539671")

# Script generated for node Change Schema - load date
ChangeSchemaloaddate_node1743500227998 = ApplyMapping.apply(frame=SelectFromCollectioncollectloaddate_node1743505539671, mappings=[("customerid", "string", "customerid", "string"), ("country", "string", "country", "string"), ("state", "string", "state", "string"), ("city", "string", "city", "string"), ("zip_code", "int", "zip_code", "int"), ("lat_long", "string", "lat_long", "string"), ("latitude", "decimal", "latitude", "decimal"), ("longitude", "decimal", "longitude", "decimal"), ("gender", "string", "gender", "string"), ("senior_citizen", "string", "senior_citizen", "string"), ("partner", "string", "partner", "string"), ("dependents", "string", "dependents", "string"), ("tenure_months", "int", "tenure_months", "int"), ("phone_service", "string", "phone_service", "string"), ("multiple_lines", "string", "multiple_lines", "string"), ("internet_service", "string", "internet_service", "string"), ("online_security", "string", "online_security", "string"), ("online_backup", "string", "online_backup", "string"), ("device_protection", "string", "device_protection", "string"), ("tech_support", "string", "tech_support", "string"), ("streaming_tv", "string", "streaming_tv", "string"), ("streaming_movies", "string", "streaming_movies", "string"), ("contract", "string", "contract", "string"), ("paperless_billing", "string", "paperless_billing", "string"), ("payment_method", "string", "payment_method", "string"), ("monthly_charges", "decimal", "monthly_charges", "decimal"), ("total_charges", "decimal", "total_charges", "decimal"), ("churn_label", "string", "churn_label", "string"), ("churn_value", "int", "churn_value", "int"), ("churn_score", "int", "churn_score", "int"), ("cltv", "int", "cltv", "int"), ("churn_reason", "string", "churn_reason", "string"), ("ingestion_date", "date", "ingestion_date", "date"), ("load_date", "date", "load_date", "date")], transformation_ctx="ChangeSchemaloaddate_node1743500227998")

# Script generated for node Custom Transform - Partition
CustomTransformPartition_node1743541972900 = partition_date(glueContext, DynamicFrameCollection({"SelectFromCollectioncollectloaddate_node1743505539671": SelectFromCollectioncollectloaddate_node1743505539671}, glueContext))

# Script generated for node Select From Collection - Partition
SelectFromCollectionPartition_node1743542109183 = SelectFromCollection.apply(dfc=CustomTransformPartition_node1743541972900, key=list(CustomTransformPartition_node1743541972900.keys())[0], transformation_ctx="SelectFromCollectionPartition_node1743542109183")

# Script generated for node Change Schema - Partition
ChangeSchemaPartition_node1743625974284 = ApplyMapping.apply(frame=SelectFromCollectionPartition_node1743542109183, mappings=[("customerid", "string", "customerid", "string"), ("country", "string", "country", "string"), ("state", "string", "state", "string"), ("city", "string", "city", "string"), ("zip_code", "int", "zip_code", "int"), ("lat_long", "string", "lat_long", "string"), ("latitude", "decimal", "latitude", "decimal"), ("longitude", "decimal", "longitude", "decimal"), ("gender", "string", "gender", "string"), ("senior_citizen", "string", "senior_citizen", "string"), ("partner", "string", "partner", "string"), ("dependents", "string", "dependents", "string"), ("tenure_months", "int", "tenure_months", "int"), ("phone_service", "string", "phone_service", "string"), ("multiple_lines", "string", "multiple_lines", "string"), ("internet_service", "string", "internet_service", "string"), ("online_security", "string", "online_security", "string"), ("online_backup", "string", "online_backup", "string"), ("device_protection", "string", "device_protection", "string"), ("tech_support", "string", "tech_support", "string"), ("streaming_tv", "string", "streaming_tv", "string"), ("streaming_movies", "string", "streaming_movies", "string"), ("contract", "string", "contract", "string"), ("paperless_billing", "string", "paperless_billing", "string"), ("payment_method", "string", "payment_method", "string"), ("monthly_charges", "decimal", "monthly_charges", "decimal"), ("total_charges", "decimal", "total_charges", "decimal"), ("churn_label", "string", "churn_label", "string"), ("churn_value", "int", "churn_value", "int"), ("churn_score", "int", "churn_score", "int"), ("cltv", "int", "cltv", "int"), ("churn_reason", "string", "churn_reason", "string"), ("ingestion_date", "date", "ingestion_date", "date"), ("year", "int", "year", "int"), ("month", "int", "month", "int")], transformation_ctx="ChangeSchemaPartition_node1743625974284")

# Script generated for node Amazon Redshift
AmazonRedshift_node1743500646591 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemaloaddate_node1743500227998, connection_type="redshift", connection_options={"postactions": "BEGIN; DELETE FROM public.customer_churn_optimized USING public.customer_churn_optimized_temp_jzqchl WHERE public.customer_churn_optimized_temp_jzqchl.customerid = public.customer_churn_optimized.customerid; INSERT INTO public.customer_churn_optimized SELECT * FROM public.customer_churn_optimized_temp_jzqchl; DROP TABLE public.customer_churn_optimized_temp_jzqchl; END;", "redshiftTmpDir": "s3://kaggle-dataset-019824224614/temp/", "useConnectionProperties": "true", "dbtable": "public.customer_churn_optimized_temp_jzqchl", "connectionName": "Redshift-connection-provisioned", "preactions": "CREATE TABLE IF NOT EXISTS public.customer_churn_optimized (customerid VARCHAR, country VARCHAR, state VARCHAR, city VARCHAR, zip_code INTEGER, lat_long VARCHAR, latitude DECIMAL, longitude DECIMAL, gender VARCHAR, senior_citizen VARCHAR, partner VARCHAR, dependents VARCHAR, tenure_months INTEGER, phone_service VARCHAR, multiple_lines VARCHAR, internet_service VARCHAR, online_security VARCHAR, online_backup VARCHAR, device_protection VARCHAR, tech_support VARCHAR, streaming_tv VARCHAR, streaming_movies VARCHAR, contract VARCHAR, paperless_billing VARCHAR, payment_method VARCHAR, monthly_charges DECIMAL, total_charges DECIMAL, churn_label VARCHAR, churn_value INTEGER, churn_score INTEGER, cltv INTEGER, churn_reason VARCHAR, ingestion_date DATE, load_date DATE); DROP TABLE IF EXISTS public.customer_churn_optimized_temp_jzqchl; CREATE TABLE public.customer_churn_optimized_temp_jzqchl AS SELECT * FROM public.customer_churn_optimized WHERE 1=2;"}, transformation_ctx="AmazonRedshift_node1743500646591")

# Script generated for node Amazon S3 - partition
EvaluateDataQuality().process_rows(frame=ChangeSchemaPartition_node1743625974284, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743541939216", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3partition_node1743542157636 = glueContext.getSink(path="s3://kaggle-dataset-019824224614/staging/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["year", "month"], enableUpdateCatalog=True, transformation_ctx="AmazonS3partition_node1743542157636")
AmazonS3partition_node1743542157636.setCatalogInfo(catalogDatabase="customer-churn-s3-glue-database-yml",catalogTableName="customer-churn-partitioned-table")
AmazonS3partition_node1743542157636.setFormat("glueparquet", compression="snappy")
AmazonS3partition_node1743542157636.writeFrame(ChangeSchemaPartition_node1743625974284)
job.commit()