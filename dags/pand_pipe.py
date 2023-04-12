from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import psycopg2


extr_path = "C://Users/Uldum/Downloads/churn_train.txt"
dest_path = "C://Users/Uldum/Proc_data/churn_train_last.csv"
db_path = "C://Users/Uldum/DWH/datascience.db"

# function to remove highly correlated columns. Will be used in main function
def trimm_correlated(df_in, threshold):
    df_corr = df_in.corr(method='pearson', min_periods=1)
    df_not_correlated = ~(df_corr.mask(np.tril(np.ones([len(df_corr)]*2, dtype=bool))).abs() > threshold).any()
    un_corr_idx = df_not_correlated.loc[df_not_correlated[df_not_correlated.index] == True].index
    df_out = df_in[un_corr_idx]
    return df_out

# main extract-transform function
def extr_transf():
    # read dataset from certain folder
    df = pd.read_csv(dest_path, delimiter='\t')
    # dind and drop duplicated rows in dataframe
    df_1 = df.drop_duplicates()
    # find and fill NA values in dataframe with mean values for each column
    for col in df_1.columns:
        df_1[col] = df_1[col].fillna(int(df_1.loc[:, col].mean()))
        # trying to transform all data in float or int if possible
        try:      
            df_1[col] = df_1[col].apply(lambda x: float(x.replace(',','.')))
        except:
            df_1[col] = df_1[col]
        try:
            df_1[col] = df_1[col].apply(lambda x: int(x))
        except:
            df_1[col] = df_1[col]
    
    #transform all categorical string in categorical int
    df_dum = pd.get_dummies(df_1)

    # find values for each columns for 2% and 98% quantilles to define outliers
    bounds = {c: dict(zip(["q1", "q3"], df_dum.approxQuantile(c, [0.02, 0.98], 0)))
    for c in df_dum.columns}

    # iqr method: find boundaryes (upper and lower) values for each col
    for c in bounds:
        iqr = bounds[c]['q3'] - bounds[c]['q1']
        bounds[c]['lower'] = bounds[c]['q1'] - (iqr * 1.5)
        bounds[c]['upper'] = bounds[c]['q3'] + (iqr * 1.5)
    
    # leave only values between boundarys removing outliers
    for column in df_dum.columns:
        df_dum = df_dum[df_dum[column]<=bounds[col]['upper']]
        df_dum = df_dum[df_dum[column]>=bounds[col]['lower']]

    # apply anti-corr function to exclude correlations between columns
    df_last = trimm_correlated(df_dum, 0.95)
    
    # load preprocessing data
    df_last.to_csv(dest_path, encoding='utf-8', index=False)


def load_data():
    conn = psycopg2.connect(db_path)
    c = conn.cursor()
    c.execute('''
                CREATE TABLE IF NOT EXISTS churn_records (
                    client_id INTEGER NOT NULL,
                    month_number INTEGER NOT NULL,
                    lifetime INTEGER NOT NULL,
                    beh_score NUMERIC NOT NULL,
                    avg_transaction_sum BIGINT,
                    salary_bucket BIGINT,
                    age INTEGER,
                    region_Moscow INTEGER,
                    region_Regions INTEGER,
                    channel_Branch INTEGER
                );
             ''')
    records = pd.read_csv(dest_path)
    records.to_sql('churn_records', conn, if_exists='append', index=True)


default_args = {
    'owner': 'me',
    'retry': 5,
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'ETL',
    default_args=default_args,
    description='First ETL with Airflow and Pandas',
    schedule=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='Extract-transform task',
    python_callable=extr_transf,
    dag=ingestion_dag,
)


task_2 = PythonOperator(
    task_id='Load to DB',
    python_callable=load_data,
    dag=ingestion_dag,
)

task_1 >> task_2