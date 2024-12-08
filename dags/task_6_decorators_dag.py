from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import logging
import random
import string
import csv

BASE_DIR = '/opt/airflow/tmp'
RAW_FILE = os.path.join(BASE_DIR, 'data.csv')
PROCESSED_DIR = os.path.join(BASE_DIR, 'processed_data')
PROCESSED_FILE = os.path.join(PROCESSED_DIR, 'data.csv')

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

@dag(
    schedule_interval=None,
    default_args=default_args,
    tags=['t1_dwh', 'task_6'],
)

def task_6_decorators_dag():
    @task
    def generate_and_save_dummy_data():

        os.makedirs(BASE_DIR, exist_ok=True)

        data = [
            {'id': i, 'value': ''.join(random.choices(string.ascii_letters + string.digits, k=10))}
            for i in range(1, 101)
        ]
        
        with open(RAW_FILE, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['id', 'value'])
            writer.writeheader()
            writer.writerows(data)
        return RAW_FILE
    
    move_file = BashOperator(
        task_id='create_dir_and_move_file',
        bash_command="""
            rm -rf {{ params.processed_dir }} &&
            mkdir -p {{ params.processed_dir }} &&
            mv {{ ti.xcom_pull(task_ids='generate_and_save_dummy_data') }} {{ params.processed_file }}
        """,
        params={
            'processed_dir': PROCESSED_DIR,
            'processed_file': PROCESSED_FILE,
        },
    )

    @task
    def load_data_to_postgres():
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS t6_table (
            id INT PRIMARY KEY,
            value VARCHAR(10)
        );
        """

        cursor.execute('TRUNCATE TABLE t6_table;')
        conn.commit()
        cursor.execute(create_table_sql)
        conn.commit()

        logging.info("Table 't6_table' created successfully.")

        with open(PROCESSED_FILE, 'r') as f:
            next(f)
            cursor.copy_expert("COPY t6_table FROM STDIN WITH CSV", f)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM t6_table;")
        row_count = cursor.fetchone()[0]
        logging.info("Number of rows inserted: %d", row_count)

        cursor.close()
        conn.close()
        return row_count
    
    @task
    def log_inserted_row_count(row_count: int):
        logging.info("Total number of rows inserted into 't6_table': %d", row_count)

    data_file  = generate_and_save_dummy_data()
    rows_inserted = load_data_to_postgres()

    data_file  >> move_file >> rows_inserted >> log_inserted_row_count(rows_inserted)

dag = task_6_decorators_dag()
