"""
Reference materials:- 
Task Groups:- https://www.astronomer.io/docs/learn/task-groups
Task Groups managing dependencies:- https://www.astronomer.io/docs/learn/managing-dependencies
defaultdict:- https://www.geeksforgeeks.org/defaultdict-in-python/


Cases:-
 1) p1 > p2 > p3 > p4
 2) Some sources in p2 are only dependent on p1 eg:- analyticsbase >> aggregated
 3) Some sources in p2 are dependent on multiple p1 sources. eg:- [tmm, analyticsbase] >> crosschannel_page
 4) Example Miscelleaneos 
"""

"""
Algorithm for complex priority mapping of the sources:-
1) Checking and preparing sources to be ingested for current composer run:-
    i) Read the FW table.
    ii) If reading the FW table pandas or polars...convert the df into DICT.
    iii) Extract data_sources, data_source_file from the dict and store it in two variables.
    iv) create the zip of the two.
    v) Use defaultdict of collection module to handle same data_sources and its sub_sources. It returns defaultdict(class<list>)
    vi) Use global priority mapping dict, which contains all the sources and its sub-sources in priority of its ingestion. We'll use this dict to filter out the sources and sub_sources for which we need to do ingestion 
    vii) Final result will be the dict contains sources and sub_sources to do actual ingestion.
2) Creating TaskGroups for the sources and sub_sources dynamically:-

"""

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import polars as pl
from collections import defaultdict

# Initializing the DAG
dag = DAG(
    dag_id='task_group_prod',
    schedule_interval=None,
    start_date=datetime(2024, 6, 18)
)

start_dag = DummyOperator(task_id='start_dag', dag=dag)
end_dag = DummyOperator(task_id='end_dag', dag=dag)

global_sources = {
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
    },
    'tmm': {
        'source_1': ['activity'],
        'source_2': [],
        'source_3': [],
        'source_4': []
    }
}

#####################################
###################################

####Implementing:- Checking and preparing sources to be ingested for current composer run   

#####################################
###################################

# Reading the fw file
df = pl.read_csv('/opt/airflow/tmp/data_sources.csv')

# Getting required fields from the table
sources = df['data_source']
sub_sources = df['sub_source']

# Creating defaultdict to store the result in default(class<list>) format: {source: [sub_sources]}

result_dict = defaultdict(list)

for key, value in zip(sources, sub_sources):
    result_dict[key].append(value)

# Filtering out the required sources only.
req_sources = global_sources

for key, values in result_dict.items():
    if key in req_sources:
        for sub_key in req_sources[key]:
            req_sources[key][sub_key] = [value for value in values if value in req_sources[key][sub_key]]

#####################################
###################################

####Implementing:- Creating TaskGroups for the sources and sub_sources dynamically 

#####################################
###################################      

# Dummy function to just print the arg passed as a dict from the taskgroup.
def print_details(**kwargs):
    print(f"{kwargs['templates_dict']['name']}")

# Dictionary to hold individual sub_groups. Later will be used to set the dependency between subsources
sub_source = {}

# Function to create the taskgroup for the sources and sub_sources dynamically.
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
# Variable to hold task group instance. Later will be used to set the dependency between the sources
task_groups = {}
for key, value in req_sources.items():
    # with TaskGroup(group_id=key, dag=dag) as parent_tg:
    for source, deps in value.items():
        if deps:  # Only create TaskGroups for non-empty dependencies
            task_groups[f'{key}_{source}'] = create_task_group(key, source, {source: deps})
    # task_groups[key] = parent_tg

# Function to set sequential dependencies within a source
def set_sequential_dependencies(key):
    source_groups = [task_groups[group] for group in task_groups if group.startswith(key)]
    for i in range(len(source_groups) - 1):
        if key in ['ipc', 'adobe'] and source_groups[i].group_id.endswith('source_1') and source_groups[i + 1].group_id.endswith('source_2'):
            sub_source['ipc_events'] >> sub_source['ipc_eventskpi']
            sub_source['adobe_analyticsbase'] >> sub_source['adobe_aggregated']
            sub_source['adobe_analyticsbase'] >> sub_source['crosschannel_crosschannel_adobe']
            sub_source['tmm_activity'] >> sub_source['crosschannel_crosschannel_adobe']
        else:
            source_groups[i] >> source_groups[i + 1]
    return source_groups

# Setting start and end dependencies dynamically
for key, value in req_sources.items():
    groups = set_sequential_dependencies(key)
    start_dag >> groups[0]
    groups[-1] >> end_dag

# start_dag >> adobe_groups[0]
# start_dag >> ipc_groups[0]

# adobe_groups[-1] >> end_dag
# ipc_groups[-1] >> end_dag

# Function to identify and connect final tasks to end_dag
def connect_final_tasks_to_end(dag, end_task):
    for task in dag.tasks:
        if not task.downstream_list and task.task_id != end_task.task_id:
            # Ensure no circular dependency is created
            if task.task_id != end_task.task_id:
                task >> end_task

# Connect final tasks to end_dag
connect_final_tasks_to_end(dag, end_dag)
