from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Initializing the DAG
dag = DAG(
    dag_id='task_group_final',
    schedule_interval=None,
    start_date=datetime(2024, 6, 18)
)

start_dag = DummyOperator(task_id='start_dag', dag=dag)
end_dag = DummyOperator(task_id='end_dag', dag=dag)

sources = {
    'adobe' : {
        'source_1': ['analyticsbase', 'check'],
        'source_2': ['aggregated', 'crosschannel_adobe', 'crosschannelpage'],
        'source_3': ['XYZ', 'demon'],
        'source_4': []
    },
    'ipc': {
        'source_1': ['channel','events'],
        'source_2': ['eventskpi'],
        'source_3': [],
        'source_4': []
    }
}




def print_details(**kwargs):
    print(f"{kwargs['templates_dict']['name']}")

 
# Dictionary to hold individual sub_groups
sub_source = {}

def create_task_group(key, group_id, dependencies):
    with TaskGroup(group_id=f'{key}_{group_id}', dag=dag) as tg:
        for key, value in dependencies.items():
            with TaskGroup(key, dag = dag) as priority_tg:
                for item in value:
                    with TaskGroup(item, dag = dag) as sub_source_tg:
                        sub_source[item] = sub_source_tg
                        # task_references[item] = item
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
                                'age': '18'
                            },
                            dag = dag
                        )
                    task_1 >> task_2
                    # task_references[item] = task_2
                    sub_source[item] = sub_source_tg    
        
    return tg

# Creating task groups dynamically
task_groups = {}
for key, value in sources.items():
    for source, deps in value.items():
        task_groups[f'{key}_{source}'] = create_task_group(key, source, {source: deps})

# Setting dependencies between task groups or consider it as p1 > p2 > p3 > p4
# 
# start_dag >> task_groups['source_1'] >> task_groups['source_2'] >> end_dag

for key in sources.keys():
    source_groups = [task_groups[group] for group in task_groups if group.startswith(key)]
    start_dag >> source_groups
    source_groups >> end_dag

# Setting sub_sources within different groups/priorities
# if 'XYZ' in sub_source:
#     sub_source['crosschannelpage'] >> sub_source['XYZ']
# task_groups['source_2'] >> sub_source['demon']
