from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from utils import airflow_comm_func


def print_hello(**kwargs):
    print("type: " + kwargs["params"]["type"])
    print("execution_date: " + str(kwargs["execution_date"]))
    print("ds "+str(kwargs['ds']))
    print("yesterday_ds "+str(kwargs['yesterday_ds']))
    print("tomorrow_ds "+str(kwargs['tomorrow_ds']))
    return 'Hello world!'


# Following are defaults which can be overridden later on
'''
default_args = {
    'owner': 'lalit.bhatt',
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 18),
    'email': ['lalit.bhatt@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
'''

default_args = {
    'description': 'Simple tutorial DAG', 
    'start_date' : datetime(2018, 4, 25),
    'owner' : 'jinks44', 
    'email' : 'jinsk44@gmail.com',
    'on_failure_callback': airflow_comm_func.on_failure_callback,
    'retries' : 1
}
dag = DAG('HelloWorld2_v1.0', default_args=default_args, schedule_interval='0 5 * * *')

dummy_task = DummyOperator(task_id='dummy_task1', retries=0, dag = dag)

hello_operator = PythonOperator(task_id = 'hello_task',
        python_callable=print_hello, 
        params={'type':'1','type2':'AA123'},
        provide_context=True,
        dag=dag)

dummy_task >> hello_operator






'''
# t1, t2, t3 and t4 are examples of tasks created using operators

t1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Hello World from Task 1"',
    dag=dag)

t2 = BashOperator(
    task_id='task_2',
    bash_command='echo "Hello World from Task 2"',
    dag=dag)

t3 = BashOperator(
    task_id='task_3',
    bash_command='echo "Hello World from Task 3"',
    dag=dag)

t4 = BashOperator(
    task_id='task_4',
    bash_command='echo "Hello World from Task 4"',
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t2)
t4.set_upstream(t3)

'''
