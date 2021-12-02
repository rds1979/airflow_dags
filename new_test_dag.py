#!/usr/bin/python3
# -*- coding: utf-8 -*-

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator