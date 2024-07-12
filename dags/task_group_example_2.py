from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import datetime

# Initializing the dag
dag = DAG(
    dag_id = 'task_group_example_2',
    schedule_interval = None,
    start_date = datetime.datetime(2024,6,18)
)

start_dag = DummyOperator(task_id = 'start_dag', dag = dag)
end_dag = DummyOperator(task_id = 'end_dag', dag = dag)

def print_details(**kwargs):
    print(f"{kwargs['templates_dict']['name']}")

# Another example of Nested Task groups:-
all_sources = { 'sources' : [
    {'adobe' : {
        'p1':['analyticsbase_p1','crosschannel_p1'],
        'p2':['aggregated_p2'],
        'p3':['check_p3','tmm_p3']
        }
    },
    {'eloqua': {
        'p1':['emailsend_p1','emailopen_p1'],
        'p2':['subscribe_p2'],
        'p3':['unsubscribe']
        } 
    },
    {'dummy': {
        'p1':[],
        'p2':['subscribe_p2','a1_p2','b1_p2'],
        'p3':[]
        } 
    } 
     
]}


with TaskGroup('Processing_DataSources', dag = dag) as process_groups:
    for index, sources in enumerate(all_sources['sources']):
        for key,value in sources.items():
            parent_source, priorities = key, value

            with TaskGroup(f'{parent_source}_job', dag = dag) as parent_process_group:

                with TaskGroup(f'{parent_source}_p1_jobs', dag = dag) as p1_jobs:

                    for sub_index, child_source in enumerate(priorities['p1']):    
                        with TaskGroup(f'{child_source}_job', dag = dag) as child_p1_process_group:

                            task_1 = PythonOperator(
                                task_id = f'{child_source}_task_1',
                                python_callable = print_details,
                                templates_dict = {
                                    'name':'Snehil',
                                    'age': '18'
                                },
                                dag = dag
                            )

                            task_2 = PythonOperator(
                                task_id = f'{child_source}_task_2',
                                python_callable = print_details,
                                templates_dict = {
                                    'name':'Mitthu',
                                    'age': '14'
                                },
                                dag = dag
                            )

                            task_1 >> task_2
                
                
                with TaskGroup(f'{parent_source}_p2_jobs', dag = dag) as p2_jobs:
                    for sub_index, child_source in enumerate(priorities['p2']):

                        with TaskGroup(f'{child_source}_job', dag = dag) as child_p2_process_group:
                            task_1 = PythonOperator(
                                task_id = f'{child_source}_task_1',
                                python_callable = print_details,
                                templates_dict = {
                                    'name':'Snehil',
                                    'age': '18'
                                },
                                dag = dag
                            )

                            task_2 = PythonOperator(
                                task_id = f'{child_source}_task_2',
                                python_callable = print_details,
                                templates_dict = {
                                    'name':'Mitthu',
                                    'age': '14'
                                },
                                dag = dag
                            )

                            task_1 >> task_2
                        
            p1_jobs >> p2_jobs
            


start_dag >> process_groups >> end_dag