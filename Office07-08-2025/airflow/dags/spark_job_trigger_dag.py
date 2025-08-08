from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="spark_eks_trigger",
    start_date=datetime(2025, 8, 8),
    schedule_interval=None,
    catchup=False
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="helm upgrade --install spark-job /opt/airflow/dags/spark-eks-helm"
    )
