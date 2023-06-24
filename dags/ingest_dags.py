from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

args = {"owner": "kremlin", "start_date":days_ago(1)}
dag = DAG(
    dag_id="ingesta_dag",
    default_args=args,
    schedule_interval='@once', # * * * * * *
)

with dag:
    run_script_ingest = BashOperator(
        task_id='run_script_ingest',
        bash_command='python "/user/app/ProyectoEndToEndPython/Proyecto/Ingesta.py"'
    )

    run_script_transform = BashOperator(
        task_id='run_script_transform',
        bash_command='python "/user/app/ProyectoEndToEndPython/Proyecto/Transformacion.py"'
    )

    run_script_validate = BashOperator(
        task_id='run_script_validate',
        bash_command='python "/user/app/ProyectoEndToEndPython/Proyecto/validate.py"'
    )

    run_script_ingest >> run_script_transform >> run_script_validate