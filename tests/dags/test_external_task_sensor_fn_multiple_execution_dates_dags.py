from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from tests.operators.sensors import TEST_DAG_ID, DEFAULT_DATE
from datetime import datetime, timedelta

args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}

bash_command_code = """
{% set s=execution_date.time().second %}
echo "second is {{ s }}"
if [[ $(( {{ s }} % 60 )) == 1 ]]
    then
        exit 1
fi
exit 0
"""
# That DAG is use to test the behavior of the ExternalTaskSensor
# when depending on several runs of an external task.
# test_without_failure should not fail, leading to
# test_external_task_sensor_multiple_dates_with_failure
# to succeed, whereas test_with_failure should fail once
# per minute (the DAG runs every second) leading to
# test_external_task_sensor_multiple_dates_with_failure
# to fail (because of timeout).
dag_external_id = TEST_DAG_ID + '_secondly_external'
dag_secondly_external = DAG(dag_external_id,
    default_args=args,
    schedule_interval=timedelta(seconds=1))
dag_secondly_external.add_task(BashOperator(
    task_id="test_with_failure",
    bash_command=bash_command_code,
    retries=0,
    depends_on_past=False,
    start_date=DEFAULT_DATE))
dag_secondly_external.add_task(DummyOperator(
    task_id="test_without_failure",
    retries=0,
    depends_on_past=False,
    start_date=DEFAULT_DATE))

dag_id = TEST_DAG_ID + '_minutely'
dag_minutely = DAG(dag_id,
    default_args=args,
    schedule_interval=timedelta(minutes=1))
dag_minutely.add_task(ExternalTaskSensor(
    task_id='test_external_task_sensor_multiple_dates_without_failure',
    external_dag_id=dag_external_id,
    external_task_id='test_without_failure',
    execution_date_fn=lambda dt: [dt + timedelta(seconds=i) for i in range(2)],
    allowed_states=['success'],
    retries=0,
    timeout=1,
    poke_interval=1,
    depends_on_past=False,
    start_date=DEFAULT_DATE))
dag_minutely.add_task(ExternalTaskSensor(
    task_id='test_external_task_sensor_multiple_dates_with_failure',
    external_dag_id=dag_external_id,
    external_task_id='test_with_failure',
    execution_date_fn=lambda dt: [dt + timedelta(seconds=i) for i in range(2)],
    allowed_states=['success'],
    retries=0,
    depends_on_past=False,
    timeout=1,
    poke_interval=1,
    start_date=DEFAULT_DATE))

