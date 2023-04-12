from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pyspark.sql.types as t
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def transform_data():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format('csv').option('header', 'true').load('C://Users/Uldum/Downloads/clustering_test_work.csv')
    prechurn = (
        df.filter(F.col('period_end') == '2022-12-07')
        .filter(F.col('recency') <= 365)
        .filter(F.col('recency') > 300)
    )
    
    out = (
        prechurn
        .groupBy('unigenders')
        .agg(
            F.count('actual_id').alias('Count'),
            F.round(F.avg(F.col('aov').cast(t.IntegerType()))).alias('Avg'),
            F.round(F.sum(F.col('aov').cast(t.IntegerType()))).alias('Sum'),
            F.max(F.col('aov').cast(t.IntegerType())).alias('Max')
        )
            
    )

    print(prechurn.select(F.col('actual_id')).distinct().count())
    print(out.show(5))
    prechurn.coalesce(1).write.format('csv').save('mycsv.csv')


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'ETL',
    default_args=default_args,
    description='First ETL with Airflow and Spark',
    schedule=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='ETL task',
    python_callable=transform_data,
    dag=ingestion_dag,
)


task_1 