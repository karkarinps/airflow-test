from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import psycopg2

# define nececesary variables
extr_path = "churn_train.txt"
dest_path = "churn_train_last.csv"
db_path = "datascience.db"
table_name = 'churn_records'
creation_table = f'''CREATE TABLE IF NOT EXISTS {table_name} ('''

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
    df = pd.read_csv(extr_path, delimiter='\t')
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
        # add column names to the creation_table string for Postgres DB
        creation_table += f"{column} DECIMAL NOT NULL, "
        creation_table = creation_table[:-2]
        creation_table += ');'

    # apply anti-corr function to exclude correlations between columns
    df_last = trimm_correlated(df_dum, 0.95)
    
    # load preprocessing data
    df_last.to_csv(dest_path, encoding='utf-8', index=False)


def load_data():
    # connect to db with posgres lib
    conn = psycopg2.connect(db_path)
    c = conn.cursor()
    # create table
    c.execute(f'''{creation_table}
             ''')
    # read and load data to SQL db
    records = pd.read_csv(dest_path)
    records.to_sql(table_name, conn, if_exists='append', index=True)


default_args = {
    'owner': 'me',
    'retry': 5,
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'ETL',
    default_args=default_args,
    description='First ETL with Airflow and Pandas',
    schedule_interval='@daily',
    catchup=False
)

task_1 = PythonOperator(
    task_id='ExtractTransformTask',
    python_callable=extr_transf,
    dag=ingestion_dag,
)


task_2 = PythonOperator(
    task_id='LoadToDB',
    python_callable=load_data,
    dag=ingestion_dag,
)

task_1 >> task_2