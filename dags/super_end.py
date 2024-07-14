from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Initializing the DAG
dag = DAG(
    dag_id='task_group_super',
    schedule_interval=None,
    start_date=datetime(2024, 6, 18)
)

start_dag = DummyOperator(task_id='start_dag', dag=dag)
end_dag = DummyOperator(task_id='end_dag', dag=dag)

sources = {
    'adobe': {
        'source_1': ['analyticsbase', 'misc_1'],
        'source_2': ['aggregated'],
        'source_3': ['misc_2', 'misc_3'],
        'source_4': []
    },
    'ipc': {
        'source_1': ['channel', 'events', 'contact', 'campaign'],
        'source_2': ['eventskpi'],
        'source_3': [],
        'source_4': []
    },
    'crosschannel': {
        'source_1': [],
        'source_2': ['crosschannel_adobe', 'crosschannel_page'],
        'source_3': [],
        'source_4': []
    },
    'eloqua': {
        'source_1': ['bounceback', 'contact', 'subscribe', 'unsubscribe'],
        'source_2': ['misc_4'],
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
            with TaskGroup(key, dag=dag) as priority_tg:
                for item in value:
                    with TaskGroup(item, dag=dag) as sub_source_tg:
                        sub_source[item] = sub_source_tg
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
                    sub_source[f"{k}_{item}"] = sub_source_tg
    return tg

# Creating task groups dynamically
task_groups = {}
for key, value in sources.items():
    for source, deps in value.items():
        if deps:  # Only create TaskGroups for non-empty dependencies
            task_groups[f'{key}_{source}'] = create_task_group(key, source, {source: deps})

# Function to set sequential dependencies within a source
def set_sequential_dependencies(key):
    source_groups = [task_groups[group] for group in task_groups if group.startswith(key)]
    for i in range(len(source_groups) - 1):
        if key in ['ipc','adobe'] and source_groups[i].group_id.endswith('source_1') and source_groups[i + 1].group_id.endswith('source_2'):
            # Ensure ipc_eventskpi only depends on ipc_events
            # sub_source['adobe_demon'] >> sub_source['ipc_events']
            sub_source['ipc_events'] >> sub_source['ipc_eventskpi']
            sub_source['adobe_analyticsbase'] >> sub_source['adobe_aggregated']
            # sub_source['adobe_analyticsbase'] >> sub_source['adobe_crosschannel_adobe']
            # sub_source['adobe_analyticsbase'] >> sub_source['adobe_crosschannelpage']
        else:
            source_groups[i] >> source_groups[i + 1]
    return source_groups

# Setting start and end dependencies dynamically
adobe_groups = set_sequential_dependencies('adobe')
ipc_groups = set_sequential_dependencies('ipc')

start_dag >> adobe_groups[0]
start_dag >> ipc_groups[0]

adobe_groups[-1] >> end_dag
ipc_groups[-1] >> end_dag

# # Adding explicit dependencies
# sub_source['adobe_demon'] >> sub_source['ipc_channel']
# sub_source['ipc_events'] >> sub_source['ipc_eventskpi']

# Function to identify and connect final tasks to end_dag
def connect_final_tasks_to_end(dag, end_task):
    for task in dag.tasks:
        if not task.downstream_list and task.task_id != end_task.task_id:
            task >> end_task

# Connect final tasks to end_dag
connect_final_tasks_to_end(dag, end_dag)
