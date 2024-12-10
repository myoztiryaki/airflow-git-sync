from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os, sys
from airflow import DAG

start_date = datetime(2024, 11, 20)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

ssh_train_password = Variable.get("ssh_train_password")
endpoint_url = Variable.get("endpoint_url")
clean_dirty_data_bucket = Variable.get("clean_dirty_data_bucket")
clean_dirty_data_key = Variable.get("clean_dirty_data_key")
access_key_id = Variable.get("access_key_id")
secret_access_key = Variable.get("secret_access_key")
SQLALCHEMY_DATABASE_URL = Variable.get("SQLALCHEMY_DATABASE_URL")


with DAG('clean_dirty_data', default_args=default_args, schedule='@once', catchup=False) as dag:
    t1 = BashOperator(
    task_id="scp_python_scrips",
    bash_command=f"sshpass -v -p {ssh_train_password} scp -o StrictHostKeyChecking=no -r /opt/airflow/code_base/airflow-git-sync/dataops_demo ssh_train@spark_client:/home/ssh_train/")

    t2 = SSHOperator(task_id='run_python', 
                    command=f"""
                    python /home/ssh_train/dataops_demo/clean_dirty_data.py \
                    -eu {endpoint_url} \
                    -b {clean_dirty_data_bucket} \
                    -k {clean_dirty_data_key} \
                    -aki {access_key_id} \
                    -sak {secret_access_key} \
                    -c {SQLALCHEMY_DATABASE_URL}
                    """,
                    conn_timeout=300,
                    cmd_timeout=300,
                    ssh_conn_id="spark_ssh_conn")



    t1 >> t2