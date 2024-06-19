import os
import yaml
import json
#
from airflow import DAG
from airflow.models import Variable
from airflow.models import Connection
from airflow import settings
#
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash_operator import BashOperator
#
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
#
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email
#
from airflow.utils.dates import days_ago


Title="GrowingStocks"

HOME = os.environ["AIRFLOW_HOME"] # retrieve the location of your home folder
dbt_project='dbtGrowingStockModules'
dbt_project_home = Variable.get('dbt_growing_stocks')
dbt_path = os.path.join(HOME,'dags', dbt_project_home)
Data=Variable.get('My_Temp_Data') 


tickers_filename=Variable.get('Ticker_filename') 
tickers_full_filename=os.path.join(HOME,'dags', Data,  tickers_filename)

OHLC_filename=Variable.get('OHLC_filename')
OHLC_full_filename=os.path.join(HOME,'dags', Data,  OHLC_filename)

OHLC_data_period=Variable.get('OHLC_data_period')


dbt_conn_id = 'dbt_snowflake_connection'

#path to SQL files used in the orchestration
growingstocks_report_sql_path = os.path.join(dbt_path, f"target/compiled/{dbt_project}/analyses/growingstocks_report.sql")

#-----------------------------------------------------------------------------------------------
def SyncConnection():
   with open(os.path.join(dbt_path,'profiles.yml')) as stream:
        try:  
            profile=yaml.safe_load(stream)[dbt_project]['outputs']['dev']

            conn_type=profile['type']
            username=profile['user']
            password=profile['password']    
            host='https://kdszsfp-nvb47448.snowflakecomputing.com/'
            role=profile['role']                     
            account=profile['account']
            warehouse=profile['warehouse']
            database=profile['database']
            schema=profile['schema']
            extra = json.dumps(dict(account=account, database=database,  warehouse=warehouse, role=role))

            session = settings.Session()           

            try:
                
                new_conn = session.query(Connection).filter(Connection.conn_id == dbt_conn_id).one()
                new_conn.conn_type = conn_type
                new_conn.login = username
                new_conn.password = password
                new_conn.host= host
                new_conn.schema = schema 
                new_conn.extra = extra
                                    
                
            except:

                new_conn = Connection(conn_id=dbt_conn_id,
                                  conn_type=conn_type,
                                  login=username,
                                  password=password,
                                  host=host,
                                  schema=schema,
                                  extra = extra
                                     )
                
            
            session.add(new_conn)
            session.commit()    

    

        
        except yaml.YAMLError as exc:
            print(exc)
#-----------------------------------------------------------------------------------------------

def Extract_OHLC(ti,**kwargs):
    from GrowingStocks.Transformations.Packages.YFinance import OHLCDataToCSV
    OHLCDataToCSV(kwargs["tickers_full_filename"],kwargs["OHLC_full_filename"],  kwargs["OHLC_data_period"])


#-----------------------------------------------------------------------------------------------
def report_failure(context):
    # include this check if you only want to get one email per DAG
    #if(task_instance.xcom_pull(task_ids=None, dag_id=dag_id, key=dag_id) == True):
    #    logging.info("Other failing task has been notified.")
    send_email = EmailOperator(to=Variable.get("send_alert_to"),
            subject=dbt_project + 'Load Failed'
            )
    send_email.execute(context)



def ReportEmail():
    email_body="<H2>Today's Growing Stocks:</H2>"
    pg_hook = SnowflakeHook(snowflake_conn_id=dbt_conn_id)
    with open(growingstocks_report_sql_path) as f: # Open sql file
        sql = f.read()
        records = pg_hook.get_records(sql)  
        email_body += "<table border=1>"  
        email_body += ("<tr><th>Ticker</th>" + 
        "<th>Growing Start Date</th>" +
        "<th>Latest Close</th></tr>")
        for record in records:  
            email_body += ("<tr><td>" + str(record[0]) + "</td>" + 
            "<td>" + str(record[1]) + "</td>"
            "<td>" + str(record[2]) + "</td></tr>")
        email_body += "</table>"
    return email_body

#----------------------------------------------- DAG --------------------------------------------
args = {
    'owner': 'airflow',
    'on_failure_callback': report_failure ,
    'email': Variable.get("send_alert_to"),
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    'start_date': days_ago(0)
}




dag = DAG(

    dag_id='GrowingStocks',

    default_args=args,

    schedule_interval='0 23 * * *',

    catchup=False

)

#Sync_Connection = PythonOperator(
#            task_id="Sync_Connection",
#            python_callable=SyncConnection)



Extract_OHLC_from_YFinance = PythonOperator(
    task_id="Extract_OHLC_from_YFinance",
    python_callable=Extract_OHLC,
    op_kwargs={"tickers_full_filename" : tickers_full_filename, "OHLC_full_filename": OHLC_full_filename, "OHLC_data_period": OHLC_data_period},
    provide_context=True,
    dag=dag
)



Load_OHLC_to_S3 = LocalFilesystemToS3Operator(
    task_id="Load_OHLC_to_S3",
    filename=OHLC_full_filename,
    dest_key="{{var.value.get('OHLC_data_daily')}}OHLC.csv",
    dest_bucket="{{var.value.get('OHLC_data_bucket')}}",
    aws_conn_id="aws_default",
    replace=True,
)

Wait_For_OHLC_Data = S3KeySensor(
        task_id='Wait_For_OHLC_Data',
        bucket_name="{{var.value.get('OHLC_data_bucket')}}",
        bucket_key="{{var.value.get('OHLC_data_daily')}}OHLC.csv",
        wildcard_match = True,
        aws_conn_id='aws_default',
        poke_interval=60,
        mode="poke",
        timeout= 600,
        # soft_fail=True,
        dag=dag
        )   

Truncate_DB_stg_Table = SQLExecuteQueryOperator(
                        task_id="Truncate_DB_stg_Table",
                        conn_id=dbt_conn_id,
                        sql="""
                                truncate mytest_db.stocks.ohlc_staging ;
                            """,
                            dag=dag)

Copy_to_DB_stg_Table = SQLExecuteQueryOperator(
                        task_id="Copy_to_DB_stg_Table",
                        conn_id=dbt_conn_id,
                        sql="""
                                copy into mytest_db.stocks.ohlc_staging 
                                from @control_db.external_stages.stocks_stage
                                file_format = (FORMAT_NAME='control_db.file_formats.csv_format_not_compressed');
                            """,
                            dag=dag)


Load_OHLC_data_from_stg = BashOperator(
    task_id='Load_OHLC_data_from_stg',
    bash_command='cd %s && dbt run --select ohlc '%dbt_path,
    dag=dag
)

Get_Growing_Stocks = BashOperator(
    task_id='Get_Growing_Stocks',
    bash_command='cd %s && dbt run --select growingstocks --vars \'{"LThreshold":"{{var.value.get(\'LThreshold\')}}","HThreshold":"{{var.value.get(\'HThreshold\')}}"}\''%dbt_path,   
    dag=dag
)
        

Remove_Data_in_S3 = SQLExecuteQueryOperator(
        task_id="Remove_Data_in_S3",
        conn_id=dbt_conn_id,
        sql="""
            REMOVE @control_db.external_stages.stocks_stage;
        """,
        dag=dag)


Compile_Report = BashOperator(
        task_id="Compile_Report",
        bash_command='cd %s && dbt compile  --select growingstocks_report'%dbt_path,
        dag=dag
        )  

Report_Email = EmailOperator(
        task_id='Report_Email',
        to="{{var.value.get('send_alert_to')}}",
        subject=dbt_project + ": Today's Growing Stocks",
        html_content=ReportEmail(),
        dag=dag
        )


Extract_OHLC_from_YFinance >> Load_OHLC_to_S3 >> Wait_For_OHLC_Data >> Truncate_DB_stg_Table >> Copy_to_DB_stg_Table >> Load_OHLC_data_from_stg >> Remove_Data_in_S3
Copy_to_DB_stg_Table >> Get_Growing_Stocks >> Remove_Data_in_S3
Remove_Data_in_S3 >> Compile_Report >> Report_Email