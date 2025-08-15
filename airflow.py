from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# Update these variables according to your environment
EKS_CLUSTER_NAME = "your-eks-cluster-name"
EKS_CLUSTER_ENDPOINT = "https://XXXXXXXX.gr7.us-east-1.eks.amazonaws.com"
AWS_REGION = "us-east-1"
AWS_ACCOUNT_ID = "123456789012"
ECR_IMAGE = "891376921217.dkr.ecr.us-east-1.amazonaws.com/spark-app-test/demo-test:latest"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_eks_job',
    default_args=default_args,
    description='Run Spark job on EKS from MWAA',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'eks', 'mwaa'],
) as dag:

    spark_job = KubernetesPodOperator(
        task_id='submit_spark_job',
        namespace='spark-app-test',
        image=ECR_IMAGE,
        cmds=["/opt/bitnami/spark/bin/spark-submit"],
        arguments=[
            "--master", f"k8s://{EKS_CLUSTER_ENDPOINT}",
            "--deploy-mode", "cluster",
            "--name", "spark-scala-job",
            "--class", "StudentFilterApp",
            "--conf", f"spark.kubernetes.container.image={ECR_IMAGE}",
            "--conf", "spark.kubernetes.namespace=spark-app-test",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa",
            "--conf", "spark.kubernetes.authenticate.executor.serviceAccountName=spark-sa",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--conf", "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=disabled",
            "--conf", "spark.kubernetes.authenticate.enable=false",
            "--conf", "spark.kubernetes.driver.annotation.sparkoperator.kubernetes.io/kerberos.enabled=false",
            "local:///opt/bitnami/spark/app/SparkS3CSVFilter-assembly-0.1.jar",
            "test-quantexa-poc/input/students.csv",
            "test-quantexa-poc/output/studentsfiltered2"
        ],
        env_vars={
            'HOME': '/tmp',
            'SPARK_JARS_IVY': '/tmp/.ivy2'
        },
        name='spark-scala-job',
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=False,  # Must be False for MWAA
        cluster_context=f"arn:aws:eks:{AWS_REGION}:{AWS_ACCOUNT_ID}:cluster/{EKS_CLUSTER_NAME}",
    )
