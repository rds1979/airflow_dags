#!/usr/bin/python3
# -*- coding: utf-8 -*-

from datetime import datetime
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner" : "airflow",
    "depends_on_past" : False,
    "email" : ['redkin.zebrainy@gmail.com'],
    "retries" : 3,
    "retry_delay" : timedelta(minutes=5)
}

@dag(default_args = default_args, schedule_interval=timedelta(hours=3),
     start_date = datetime(2021, 6, 9), tags = ['skazbuka', 'postgresql'])
def load_abtest_values():
    @task
    def extract_external_values() -> list:
        postgres_conn_id = 'postgres_external',
        sql = '''
            SELECT * FROM sec.user_config;
        '''
        result = sql
        return result

    @task()
    def drop_internal_table():
        postgres_conn_id = 'postgres_internal'
        sql = '''
            DROP TABLE IF EXISTS load.test_users;
        '''

    @task()
    def create_internal_table():
        postgres_conn_id = 'postgres_internal'
        sql = '''
            CREATE TABLE load.test_users(LIKE amplitude.users);
        '''

    @task
    def populate_internal_table(abtests: list):
        pass

    @task()
    def analyze_internal_table():
        postgres_conn_id = 'postgres_internal'
        sql = '''
            ANALYZE TABLE load.test_users;
        '''

    abtests = extract_external_values()
    drop_internal_table()
    create_internal_table()
    populate_internal_table(abtests)
    analyze_internal_table()

load_abtests_dag = load_abtest_values()