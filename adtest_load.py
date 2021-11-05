#!/usr/bin/python3
# -*- coding: utf-8 -*-

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ['redkin.zebrainy@gmail.com'],
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id = "load_abtest_values",
    start_date = datetime(2021, 7, 25),
    schedule_interval = "@daily",
    default_args = default_args,
    catchup = False,
    tags = ['skazbuka', 'postgresql']
)as dag:

    truncate = PostgresOperator(
        task_id="truncate_users",
        postgres_conn_id="postgres_internal",
        sql="""
            TRUNCATE TABLE amplitude.users;
        """
    )

    insert = PostgresOperator(
        task_id="insert_abtests",
        postgres_conn_id="postgres_internal",
        sql='''
            INSERT INTO amplitude.users SELECT * FROM fdw_import.user_config;
        '''
    )

    analyze = PostgresOperator(
        task_id="analyze_users",
        postgres_conn_id="postgres_internal",
        sql='''
            ANALYZE amplitude.users;;
        '''
    )

    statinfo = PostgresOperator(
        task_id="add_operation_info",
        postgres_conn_id="postgres_internal",
        sql='''
            INSERT INTO airflow.operation_info (information) VALUES ($$Loading ABTests information$$);
        '''
    )

    truncate >> insert >> analyze >> statinfo
