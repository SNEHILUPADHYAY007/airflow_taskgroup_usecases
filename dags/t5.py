from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Initializing the DAG
dag = DAG(
    dag_id='task_group_example_5',
    schedule_interval=None,
    start_date=datetime(2024, 6, 18)
)

start_dag = DummyOperator(task_id='start_dag', dag=dag)
end_dag = DummyOperator(task_id='end_dag', dag=dag)

adobe = {
    'source_1': ['analyticsbase'],
    'source_2': ['aggregated', 'crosschannel_adobe', 'crosschannelpage'],
    'source_3': ['XYZ'],
    'source_4': []
}

ipc = {
    'source_1': ['channel', 'campaign']
}

def create_task_group(group_id, dependencies):
    with TaskGroup(group_id=group_id, dag=dag) as tg:
        tasks = {}
       
        for task_id, dep_list in dependencies.items():
            tasks[task_id] = DummyOperator(task_id=task_id, dag=dag)
            for dep in dep_list:
                if dep not in tasks:
                    tasks[dep] = DummyOperator(task_id=dep, dag=dag)
        
        for task_id, dep_list in dependencies.items():
            for dep in dep_list:
                tasks[dep] >> tasks[task_id]
    return tg

# Creating task groups dynamically
task_groups = {}
for source, deps in adobe.items():
    task_groups[source] = create_task_group(source, {source: deps})

# Setting dependencies between task groups
start_dag >> task_groups['source_1'] >> task_groups['source_2'] >> end_dag

# Dynamically setting dependencies among task groups
for source, deps in adobe.items():
    for dep in deps:
        if dep in task_groups and dep:
            task_groups[source] >> task_groups[dep]

# Setting start and end dependencies
start_dag >> list(task_groups.values())
list(task_groups.values()) >> end_dag

# Setting cross-group dependencies
# task_groups['source_2'].crosschannelpage >> task_groups['source_3'].XYZ
