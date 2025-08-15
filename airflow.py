from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

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
    'spark_eks_mwaa',
    default_args=default_args,
    description='MWAA-optimized Spark on EKS',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'eks', 'mwaa'],
) as dag:

    # Combined task to create resources and submit job
    spark_job = KubernetesPodOperator(
        task_id='spark_eks_job',
        namespace='spark-app-test',
        image='891376921217.dkr.ecr.us-east-1.amazonaws.com/spark-app-test/demo-test:latest',
        cmds=["sh", "-c"],
        arguments=[
            """
            # Create service account if not exists
            kubectl create serviceaccount spark-sa -n spark-app-test --dry-run=client -o yaml | kubectl apply -f - && \
            
            # Create role binding if not exists
            kubectl create clusterrolebinding spark-role-binding \
              --clusterrole=edit \
              --serviceaccount=spark-app-test:spark-sa \
              --dry-run=client -o yaml | kubectl apply -f - && \
            
            # Submit Spark job
            /opt/bitnami/spark/bin/spark-submit \
              --master k8s://https://0503A10B2E2E51644A2804C29C3A77EC.gr7.us-east-1.eks.amazonaws.com \
              --deploy-mode cluster \
              --name spark-scala-job \
              --class StudentFilterApp \
              --conf spark.kubernetes.container.image=891376921217.dkr.ecr.us-east-1.amazonaws.com/spark-app-test/demo-test:latest \
              --conf spark.kubernetes.namespace=spark-app-test \
              --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
              --conf spark.kubernetes.authenticate.executor.serviceAccountName=spark-sa \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --conf spark.driver.extraJavaOptions=-Djava.security.auth.login.config=disabled \
              --conf spark.kubernetes.authenticate.enable=false \
              --conf spark.kubernetes.driver.annotation.sparkoperator.kubernetes.io/kerberos.enabled=false \
              local:///opt/bitnami/spark/app/SparkS3CSVFilter-assembly-0.1.jar \
              test-quantexa-poc/input/students.csv \
              test-quantexa-poc/output/studentsfiltered2
            """
        ],
        env_vars={
            'HOME': '/tmp',
            'SPARK_JARS_IVY': '/tmp/.ivy2'
        },
        name='spark-scala-job-mwaa',
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,  # MWAA-specific setting
        cluster_context='aws',  # MWAA-specific setting
    )