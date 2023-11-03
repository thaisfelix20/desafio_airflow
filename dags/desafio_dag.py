from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

import sqlite3

import pandas as pd

from pandasql import sqldf

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ler_order_escrever_output():
    try: 
        #ler_order
        conexao = sqlite3.connect(
            "data/Northwind_small.sqlite",
            isolation_level=None,
            detect_types=sqlite3.PARSE_COLNAMES
            )

        order = pd.read_sql_query("SELECT * FROM 'Order';", conexao)
        
        conexao.close()
        
        #escrever_output
        order.to_csv('output/output_orders.csv', index=False)

    except Exception as e:
        print(e)

def ler_orderdetails_join_order_csv_escrever_txt():
    try: 
        #ler_orderdetails
        conexao = sqlite3.connect(
            "data/Northwind_small.sqlite",
            isolation_level=None,
            detect_types=sqlite3.PARSE_COLNAMES
            )
        
        order_detail = pd.read_sql_query("SELECT * FROM OrderDetail;", conexao)

        conexao.close()

        #join_order_csv
        order_csv = pd.read_csv('output/output_orders.csv')

        query = '''
            SELECT sum(od.Quantity) AS total
            FROM order_csv o 
            LEFT JOIN order_detail od ON o.id = od.OrderId 
            WHERE o.ShipCity = 'Rio de Janeiro'
            '''
        
        quantidade = sqldf(query).values[0][0]

        #escrever_txt
        with open('output/count.txt', mode='w') as f:
            quantidade = str(quantidade)
            f.write(quantidade)

    except Exception as e:
        print(e)

## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('output/count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("output/final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'MeuDesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    task1 = PythonOperator(
        task_id='task1',
        python_callable=ler_order_escrever_output,
        provide_context=True
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=ler_orderdetails_join_order_csv_escrever_txt,
        provide_context=True
    )
    
task1 >> task2 >> export_final_output