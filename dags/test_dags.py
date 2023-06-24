from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

args = {"owner": "kremlin", "start_date":days_ago(1)}
dag = DAG(
    dag_id="Test_Dag1",
    default_args=args,
    schedule_interval='@once', # * * * * * *
)

with dag:
    run_script_print = BashOperator(
        task_id='run_script_print',
        bash_command='python "/user/app/ProyectoEndToEndPython/Clases/3.Data Scheduling/test_dags.py"'
    )
