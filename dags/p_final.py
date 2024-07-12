from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Initializing the DAG
dag = DAG(
    dag_id='task_group_last_option',
    schedule_interval=None,
    start_date=datetime(2024, 6, 18)
)

start_dag = DummyOperator(task_id='start_dag', dag=dag)
end_dag = DummyOperator(task_id='end_dag', dag=dag)

sources = {
    'adobe': {
        'source_1': ['analyticsbase', 'check'],
        'source_2': ['aggregated', 'crosschannel_adobe', 'crosschannelpage'],
        'source_3': ['XYZ', 'demon'],
        'source_4': []
    },
    'ipc': {
        'source_1': ['channel', 'events'],
        'source_2': ['eventskpi'],
        'source_3': [],
        'source_4': []
    }
}

def print_details(**kwargs):
    print(f"{kwargs['templates_dict']['name']}")

# Dictionary to hold individual sub_groups
sub_source = {}

def create_task_group(k, group_id, dependencies):
    with TaskGroup(group_id=f'{k}_{group_id}', dag=dag) as tg:
        for key, value in dependencies.items():
            with TaskGroup(group_id=key, dag=dag) as priority_tg:
                for item in value:
                    with TaskGroup(group_id=item, dag=dag) as sub_source_tg:
                        sub_source[f"{k}_{item}"] = sub_source_tg
                        task_1 = PythonOperator(
                            task_id='task_1',
                            python_callable=print_details,
                            templates_dict={
                                'name': 'Snehil',
                                'age': '18'
                            },
                            dag=dag
                        )
                        task_2 = PythonOperator(
                            task_id='task_2',
                            python_callable=print_details,
                            templates_dict={
                                'name': 'Mitthu',
                                'age': '18'
                            },
                            dag=dag
                        )
                        task_1 >> task_2
    return tg

# Creating task groups dynamically
task_groups = {}
for key, value in sources.items():
    with TaskGroup(group_id=key, dag=dag) as parent_tg:
        for source, deps in value.items():
            if deps:  # Only create TaskGroups for non-empty dependencies
                task_groups[f'{key}_{source}'] = create_task_group(key, source, {source: deps})
    task_groups[key] = parent_tg

# Function to set sequential dependencies within a source
def set_sequential_dependencies(key):
    source_groups = [task_groups[group] for group in task_groups if group.startswith(f"{key}_source")]
    for i in range(len(source_groups) - 1):
        source_groups[i] >> source_groups[i + 1]
    return source_groups

# Setting start and end dependencies dynamically
adobe_groups = set_sequential_dependencies('adobe')
ipc_groups = set_sequential_dependencies('ipc')

for i in ['adobe','ipc']:
    start_dag >> task_groups[f"{i}"]
    task_groups[f"{i}"] >> end_dag
# start_dag >> task_groups['ipc']

# task_groups['adobe'] >> end_dag


# Adding explicit dependencies
sub_source['adobe_demon'] >> sub_source['ipc_channel']
