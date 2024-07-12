from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import datetime

# Initializing the dag
dag = DAG(
    dag_id = 'task_group_example_3',
    schedule_interval = None,
    start_date = datetime.datetime(2024,6,18)
)

start_dag = DummyOperator(task_id = 'start_dag', dag = dag)
end_dag = DummyOperator(task_id = 'end_dag', dag = dag)

def print_details(**kwargs):
    print(f"{kwargs['templates_dict']['name']}")

# Another example of Nested Task groups:-
adobe = ['analyticsbase', 'aggregated']
crosschannel = ['crosschannel_adobe', 'crosschannelpage']
ipc = ['channel','campaign']

with TaskGroup('data_ingestion', dag = dag) as data_ingestion:

    ipc_tg = None

    with TaskGroup('adobe_and_crosschannel', dag = dag) as adobe_crosschannel:
        
        with TaskGroup('analyticsbase', dag = dag) as adobe_analyticsbase:
            analyticsbase_1 = DummyOperator(task_id="process_analyticsbase", dag = dag)

        with TaskGroup('aggregated', dag = dag) as adobe_aggregated:
            aggregated_1 = DummyOperator(task_id="process_aggregated", dag = dag)

        if 'analyticsbase' in adobe:
            if 'aggregated' in adobe:
                adobe_analyticsbase >> adobe_aggregated
            else:
                adobe_analyticsbase
        
        
        
    with TaskGroup('cross_channel', dag = dag) as cross_channel:
        crosschannel_adobe_tg = None
        crosschannel_page_tg = None

        if 'crosschannel_adobe' in crosschannel:
            with TaskGroup('crosschannel_adobe', dag=dag) as crosschannel_adobe:
                crosschannel_1 = DummyOperator(task_id="process_crosschannel", dag=dag)
            crosschannel_adobe_tg = crosschannel_adobe

        if 'crosschannelpage' in crosschannel:
            with TaskGroup('crosschannel_page', dag=dag) as crosschannel_page:
                crosschannelpage_1 = DummyOperator(task_id="process_crosschannelpage", dag=dag)
            crosschannel_page_tg = crosschannel_page

        # Create a list of task groups that are not None
        crosschannel_task_groups = [tg for tg in [crosschannel_adobe_tg, crosschannel_page_tg] if tg is not None]

        # If there are any task groups, set dependencies
        if crosschannel_task_groups:
            adobe_analyticsbase >> crosschannel_task_groups  

    
    with TaskGroup('ipc', dag = dag) as ipc_source:
        ipc_channel_tg = None
        ipc_camp_tg = None

        if 'channel' in ipc:
            with TaskGroup('ipc_channel', dag=dag) as ipc_channel:
                ipc_channel_1 = DummyOperator(task_id="process_ipc_channel", dag=dag)
            ipc_channel_tg = ipc_channel

        if 'campaign' in ipc:
            with TaskGroup('campaign_ipc', dag=dag) as campaign_ipc:
                ipc_campaign_1 = DummyOperator(task_id="process_campaign_ipc", dag=dag)
            ipc_camp_tg = campaign_ipc

    ipc_source
    # adobe_crosschannel >> cross_channel

start_dag >> data_ingestion >> end_dag

