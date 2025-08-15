from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'spark_eks_production',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'production']
) as dag:

    SparkJob = KubernetesPodOperator(
        task_id='run_spark_etl',
        namespace='spark-app-test',
        image='891376921217.dkr.ecr.us-east-1.amazonaws.com/spark-app-test/demo-test:latest',
        cmds=["/opt/bitnami/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://E4D2GEACE1D1C5654CDDC0B87544FC6E.gr7.us-east-1.eks.amazonaws.com",
            "--deploy-mode", "cluster",
            "--name", "spark-production-job",
            "--class", "StudentFilterApp",
            "--conf", "spark.kubernetes.container.image=891376921217.dkr.ecr.us-east-1.amazonaws.com/spark-app-test/demo-test:latest",
            "--conf", "spark.kubernetes.namespace=spark-app-test",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "local:///opt/bitnami/spark/app/SparkS3CSVFilter-assembly-0.1.jar",
            "s3://test-quantexa-poc/input/students.csv",
            "s3://test-quantexa-poc/output/studentsfiltered2"
        ],
        name="spark-production-pod",
        in_cluster=False,
        cluster_context="arn:aws:eks:us-east-1:891376921217:cluster/spark-eks-cluster",
        get_logs=True,
        is_delete_operator_pod=True,
        env_vars={
            'HOME': '/tmp',
            'SPARK_JARS_IVY': '/tmp/.ivy2'
        }
    )
