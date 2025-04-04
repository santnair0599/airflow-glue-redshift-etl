from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

def glue_job_s3_redshift_transfer(job_name, **kwargs):
    session = AwsGenericHook(aws_conn_id='aws_s3_conn')

    # Get a client in the same region as Glue job
    boto3_session = session.get_session(region_name='us-east-1')

    # Trigger the job using its name
    client = boto3_session.client('glue')
    client.start_job_run(
        JobName = job_name,
    )



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'email': ['myemaildomain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15) 
}

With DAG('my_dag',
    default_args=default_args,
    schedule_interval = '@weekly',
    catchup = False) as dag:

    glue_job_trigger = PythonOperator(
    task_id = 'tsk_glue_job_trigger',
    python_callable = glue_job_s3_redshift_transfer,
    op_kwargs = (
        'job_name': 's3_upload_to_redshift_gluejob'
    ),
    )