from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.pythonoperator import PythonOperator
import yfinance as yf
import pandas as pd
import os
import glob
  
  
path = os.getcwd()
csv_files = glob.glob(os.path.join('data', "*.csv"))


now = datetime.now()


def down_mrkt_data(stock, filename):
    start_date = now.strftime("%Y-%m-%d")
    end_date = now + timedelta(days=1)
    end_date = end_date.strftime("%Y-%m-%d")
    tsla_df = yf.download(stock, start=start_date, end=end_date, interval='1m')
    tsla_df.to_csv(f'{filename}.csv', header=False)


def query_data(csv_files):
    names = ['data_time', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
    count = 0
    for file in csv_files:
        count += 1
        df = pd.read.csv(file, names)
        max_high = df['high']
        max_high = max_high.max()
        min_low = df['low']
        min_low = min_low.max()
        with (f'query/query{}.txt', 'w') as f:
            f.write(
                f"""
                Results for {file}:
                Maximum High: {max_high}
                Minimum Low: {min_low}
                """
                )


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'marketvol',
    default_args=default_args,
    description='A simple DAG',
    start_date = now.strftime("%Y-%m-%d"),
    schedule_interval='0 18 * * 1-5',
) as dag:

    t0 = BashOperator(
        task_id='create_temp_dir',
        bash_command=f'mkdir -p /tmp/data/{now.strftime("%Y-%m-%d")}',
        retries=3
    )

    t1 = PythonOperator(
        task_id='down_mrkt_data_one',
        python_callable=down_mrkt_data,
        op_kwargs={'stock': 'TSLA', 'filename': 'TSLA_data'}
    )

    t2 = PythonOperator(
        task_id='down_mrkt_data_two',
        python_callable=down_mrkt_data,
        op_kwargs={'stock': 'AAPL', 'filename': 'AAPL_data'}
    )

    t3 = BashOperator(
        task_id='move_mrkt_data_one',
        bash_command='mv TSLA_data.csv data/'
    )

    t4 = BashOperator(
        task_id='move_mrkt_data_two',
        bash_command='mv AAPL_data.csv data/'
    )

    t5 = PythonOperator(
        task_id='query_mrkt_data',
        python_callable=query_data,
        op_kwargs={'csv_files': 'csv_files'}
    )

    t0 >> [t1, t2]
    t1 >> t3
    t2 >> t4
    [t3, t4] >> t5