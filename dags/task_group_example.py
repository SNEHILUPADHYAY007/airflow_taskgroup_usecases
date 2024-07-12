from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import datetime

# Initializing the dag
dag = DAG(
    dag_id = 'task_group_example',
    schedule_interval = None,
    start_date = datetime.datetime(2024,6,18)
)

start_dag = DummyOperator(task_id = 'start_dag', dag = dag)
end_dag = DummyOperator(task_id = 'end_dag', dag = dag)
# Sample example for a Task group

# Function to print the info passed using template_dict
def print_details(**kwargs):
    print(f"{kwargs['templates_dict']['name']}")

# Using taskgroup
with TaskGroup('task_group_example', dag = dag) as task_group_example:
    task_1 = PythonOperator(
        task_id = 'task_1',
        python_callable = print_details,
        templates_dict = {
            'name':'Snehil',
            'age': '18'
        },
        dag = dag
    )

    task_2 = PythonOperator(
        task_id = 'task_2',
        python_callable = print_details,
        templates_dict = {
            'name':'Mitthu',
            'age': '14'
        },
        dag = dag
    )

    
    
# Using Nested TaskGroup
with TaskGroup('series_and_parallel_task_group', dag = dag) as series_and_parallel_task_group:
    # Task Group_1
    with TaskGroup('series_group', dag = dag) as series_group:
        task_1 = PythonOperator(
            task_id = 'task_1',
            python_callable = print_details,
            templates_dict = {
                'name':'Snehil',
                'age': '18'
            },
            dag = dag
        )

        task_2 = PythonOperator(
            task_id = 'task_2',
            python_callable = print_details,
            templates_dict = {
                'name':'Mitthu',
                'age': '14'
            },
            dag = dag
        )

        task_1 >> task_2

    # Task Group_2
    with TaskGroup('parallel_group', dag = dag) as parallel_group:
        task_1 = PythonOperator(
            task_id = 'task_1',
            python_callable = print_details,
            templates_dict = {
                'name':'Snehil',
                'age': '18'
            },
            dag = dag
        )

        task_2 = PythonOperator(
            task_id = 'task_2',
            python_callable = print_details,
            templates_dict = {
                'name':'Mitthu',
                'age': '14'
            },
            dag = dag
        )

# Another example of Nested Task groups:-
all_sources = { 'sources' : [
    {'adobe' : ['analyticsbase','aggregated','check']},
    {'onetrust' : ['datasubjects','purposes','preferences']},
    {'eloqua': ['emailsend','emailopen','subscribe','unsubscribe']}
]}

with TaskGroup('Processing_Sources', dag = dag) as processing_sources:
    for index,source in enumerate(all_sources['sources']):
        for key, value in source.items():
            parent_source, child_sources = key, value

        

            with TaskGroup(parent_source, dag = dag) as parent_group:
                previous_group = None
                
                for source in child_sources:
                    with TaskGroup(f'{source}', dag = dag) as source_group:
                        task_1 = PythonOperator(
                            task_id = f'{source}_task_1',
                            python_callable = print_details,
                            templates_dict = {
                                'name':'Snehil',
                                'age': '18'
                            },
                            dag = dag
                        )

                        task_2 = PythonOperator(
                            task_id = f'{source}_task_2',
                            python_callable = print_details,
                            templates_dict = {
                                'name':'Mitthu',
                                'age': '14'
                            },
                            dag = dag
                        )

                        task_1 >> task_2
                        if parent_source == 'adobe':
                            if previous_group:
                                previous_group >> source_group
                            previous_group = source_group
                        else:
                            source_group


# Task Running sequence
start_dag >> processing_sources >> end_dag