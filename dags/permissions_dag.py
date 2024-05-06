import re
import zipfile
from datetime import date, datetime, timedelta
from itertools import product
import os

import great_expectations as ge
import pandas as pd
import pandas_gbq
import requests

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from great_expectations.dataset import PandasDataset
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.render.renderer import *
from great_expectations.render.view import DefaultJinjaPageView
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from IPython import display

permission_types = ["budowa", "rozbudowa", "odbudowa", "nadbudowa"]

categories = [
    "I",
    "II",
    "III",
    "IV",
    "V",
    "VI",
    "VII",
    "VIII",
    "IX",
    "X",
    "XI",
    "XII",
    "XIII",
    "XIV",
    "XV",
    "XVI",
    "XVII",
    "XVIII",
    "XIX",
    "XX",
    "XXI",
    "XXII",
    "XXIII",
    "XXIV",
    "XXV",
    "XXVI",
    "XXVII",
    "XXVIII",
    "XXIX",
    "XXX",
]

default_args = {"owner": "Piotr", "retries": 2, "retry_delay": timedelta(minutes=2)}


def permission_download():
    url = "https://wyszukiwarka.gunb.gov.pl/pliki_pobranie/wynik_zgloszenia.zip"
    r = requests.get(url)
    with open("/home/airflow/data.zip", "wb") as f:
        f.write(r.content)

    with zipfile.ZipFile("/home/airflow/data.zip", "r") as zip_file:
        file_name = zip_file.namelist()[0]
        zip_file.extract(file_name)


def data_validation(**context):
    global categories, permission_types
    df = pd.read_csv("/opt/airflow/wynik_zgloszenia.csv", sep="#")
    current_date = pd.to_datetime(context["ds"])
    first_day_last_month = current_date - pd.offsets.MonthBegin(2)
    first_day_current_month = current_date - pd.offsets.MonthBegin(1)
    df["data_wplywu_wniosku_do_urzedu"] = pd.to_datetime(
        df["data_wplywu_wniosku_do_urzedu"]
    )
    df = df[
        (df["data_wplywu_wniosku_do_urzedu"] >= first_day_last_month)
        & (df["data_wplywu_wniosku_do_urzedu"] < first_day_current_month)
    ]

    df["rodzaj_zam_budowlanego"] = df["rodzaj_zam_budowlanego"].astype(str)
    df["rodzaj_zam_budowlanego_new"] = df["rodzaj_zam_budowlanego"].apply(
        lambda x: x.split(" ", 1)[0] if x else None
    )
    ge_df = ge.from_pandas(df)
    ge_df.expect_column_values_to_be_in_set(
        "rodzaj_zam_budowlanego_new", permission_types
    )
    ge_df.expect_column_values_to_be_in_set("kategoria", categories)
    ge_df.expect_column_values_to_be_in_set(
        "wojewodztwo_objekt",
        [
            "mazowieckie",
            "kujawsko-pomorskie",
            "dolnośląskie",
            "lubelskie",
            "lubuskie",
            "pomorskie",
            "małopolskie",
            "łódzkie",
            "warmińsko-mazurskie",
            "opolskie",
            "wielkopolskie",
            "podkarpackie",
            "śląskie",
            "świętokrzyskie",
            "podlaskie",
            "zachodniopomorskie",
        ],
    )
    ge_df.expect_column_values_to_not_be_null(
        "data_wplywu_wniosku_do_urzedu", mostly=0.95
    )
    ge_df.expect_column_values_to_not_be_null("numer_ewidencyjny_system", mostly=0.95)
    ge_df.expect_column_values_to_not_be_null("terc", mostly=0.95)
    ge_df.expect_table_row_count_to_be_between(min_value=1000, max_value=None)
    ge_df.expect_column_values_to_match_regex(
        column="obiekt_kod_pocztowy", regex=r"^\d{2}-\d{3}$", mostly=0.95
    )

    expectation_suite_name = "data_validation_expectations"
    ge_df.save_expectation_suite(expectation_suite_name)

    validation_results = ge_df.validate(expectation_suite_name)
    document_model = ValidationResultsPageRenderer().render(validation_results)

    with open("validation_report.html", "w") as f:
        f.write(DefaultJinjaPageView().render(document_model))


def permissions_to_gbq(**context):
    """
    Function is responsible for uploading bulding permissions dataframe to GoogleBigQuery database.

    Args:
        context: The Airflow context dictionary containing matedate and rintime information regarding current execution of a workflow task

    Returns:
    """

    credentials = service_account.Credentials.from_service_account_file(
        f"/{os.environ.get('GOOGLE_CLOUD_JSON_KEY')}",
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    query_job = client.query(
        f"""
        select * from permissions-417120.permissions_dataset.permissions
    """,
        location="US",
    )

    rows = list(query_job.result())
    total_rows = len(rows)

    df = pd.read_csv("/opt/airflow/wynik_zgloszenia.csv", sep="#")
    df["data_wplywu_wniosku_do_urzedu"] = pd.to_datetime(
        df["data_wplywu_wniosku_do_urzedu"]
    )
    df = df.drop_duplicates(subset=["numer_ewidencyjny_system"])
    df = df[
        df["numer_ewidencyjny_system"].apply(
            lambda x: re.match(r".*/([0-9]*/[0-9]{4})$", x) is not None
        )
    ]
    df["terc"] = df["terc"].astype(str)
    df = df.rename(columns={"cecha.1": "cecha_1"})

    if total_rows == 0:
        pandas_gbq.to_gbq(
            df,
            "permissions-417120.permissions_dataset.permissions",
            project_id="permissions-417120",
            if_exists="replace",
            location="US",
            credentials=credentials,
        )
    else:
        current_date = pd.to_datetime(context["ds"])
        first_day_last_month = current_date - pd.offsets.MonthBegin(2)
        first_day_current_month = current_date - pd.offsets.MonthBegin(1)
        df = df[
            (df["data_wplywu_wniosku_do_urzedu"] >= first_day_last_month)
            & (df["data_wplywu_wniosku_do_urzedu"] < first_day_current_month)
        ]
        pandas_gbq.to_gbq(
            df,
            "permissions-417120.permissions_dataset.permissions",
            project_id="permissions-417120",
            if_exists="append",
            location="US",
            credentials=credentials,
        )


def permission_df_formatting(df, context):
    """
    Function is reponsible for cleansing and formatting initial data

    Args:
        df: source Dataframe consisting of all permissions
        context: The Airflow context dictionary containing various information regarding current execution of a workflow task

    Returns:
        df: source Dataframe consisting of all permissions
    """
    execution_date = pd.to_datetime(context["ds"])
    df["data_wplywu_wniosku_do_urzedu"] = pd.to_datetime(
        df["data_wplywu_wniosku_do_urzedu"]
    )

    df["months_diff"] = (
        execution_date.year - df["data_wplywu_wniosku_do_urzedu"].dt.year
    ) * 12 + (execution_date.month - df["data_wplywu_wniosku_do_urzedu"].dt.month)

    df["rodzaj_zam_budowlanego"] = df["rodzaj_zam_budowlanego"].apply(
        lambda x: x.split(" ", 1)[0] if x else None
    )
    df["terc"] = df["terc"].astype(str)
    df["terc"] = df["terc"].apply(lambda x: x.split(".", 1)[0])
    return df


def create_pivot_table(df, df_final, permission_type, time_period, category=None):
    """
    Function is reponsible for making pivot from dataframe with particular filters in order to calculate aggregated values

    Args:
        df: source Dataframe consisting of all permissions
        df_final: Target dataframe with all aggregates calculated
        permission_type: Permission type for which we calculate aggregate
        time_period: Time period for which we calculate aggregate
        category [Optional]: Category period for which we calculate aggregate

    Returns:
        df_final: Target dataframe with all aggregates calculated
    """
    if category:
        element = f"{permission_type}_kat_{category}_{time_period}"
        temp_df = df[
            (df["kategoria"] == category)
            & (df["rodzaj_zam_budowlanego"] == permission_type)
            & (df["months_diff"] <= int(time_period.split("_")[1]))
        ]
    else:
        element = f"{permission_type}_{time_period}"
        temp_df = df[
            (df["rodzaj_zam_budowlanego"] == permission_type)
            & (df["months_diff"] <= int(time_period.split("_")[1]))
        ]

    table = pd.pivot_table(
        temp_df,
        values="numer_ewidencyjny_system",
        index="terc",
        aggfunc="count",
        fill_value=0,
    )
    table.reset_index(inplace=True)
    table = table.rename(columns={"numer_ewidencyjny_system": element})

    if table.empty:        
        df_final[element] = 0
    else:
        df_final = df_final.fillna(0)
        df_final = df_final.merge(table, on="terc", how="left")


    return df_final


def permissions_aggregate_values_calculation(df, context):
    """
    Function is reponsible for calculating aggregated values

    Args:
        df: source Dataframe consisting of all permissions
        context: The Airflow context dictionary containing various information regarding current execution of a workflow task

    Returns:
        df_final: Dataframe containing calculated aggregates
    """
    global permission_types, categories
    time_periods = ["last_1_month", "last_2_months", "last_3_months"]
    df["data_wplywu_wniosku_do_urzedu"] = df[
        "data_wplywu_wniosku_do_urzedu"
    ].dt.strftime("%Y-%m")
    df_unique_terc_list = df["terc"].unique().tolist()
    df_final = pd.DataFrame({"terc": df_unique_terc_list})
    df_final["terc"] = df_final["terc"].astype(str)
    df_final["terc"] = df_final["terc"].apply(lambda x: "0" + x if len(x) == 6 else x)

    for permission_type in permission_types:
        for time_period in time_periods:
            df_final = create_pivot_table(df, df_final, permission_type, time_period)         
            for category in categories:
                df_final = create_pivot_table(
                    df, df_final, permission_type, time_period, category
                )                

    voivodeship_terc_grouping_df = df_final.copy()
    county_terc_grouping_df = df_final.copy()

    voivodeship_terc_grouping_df["terc"] = voivodeship_terc_grouping_df["terc"].str[:2]
    county_terc_grouping_df["terc"] = county_terc_grouping_df["terc"].str[:4]

    voivodeship_grouped_df = voivodeship_terc_grouping_df.groupby(
        "terc", as_index=False
    ).sum()
    county_grouped_df = county_terc_grouping_df.groupby("terc", as_index=False).sum()

    df_final = pd.concat([df_final, county_grouped_df, voivodeship_grouped_df])

    previous_month_execution_date = pd.to_datetime(
        context["ds"]
    ) - pd.offsets.MonthBegin(2)
    df_final.insert(0, "data_wplywu_wniosku_do_urzedu", previous_month_execution_date)

    return df_final


def aggregates_calculation(**context):
    df = pd.DataFrame()

    execution_date = pd.to_datetime(context["ds"])
    first_day_three_months_ago = execution_date - pd.offsets.MonthBegin(4)
    first_day_current_month = execution_date - pd.offsets.MonthBegin(1)

    credentials = service_account.Credentials.from_service_account_file(
        f"/{os.environ.get('GOOGLE_CLOUD_JSON_KEY')}",
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )

    query = f"""
        SELECT *
        FROM permissions-417120.permissions_dataset.permissions
        WHERE data_wplywu_wniosku_do_urzedu >= '{first_day_three_months_ago}'
        AND data_wplywu_wniosku_do_urzedu < '{first_day_current_month}'
    """

    chunk = pandas_gbq.read_gbq(
        query, project_id=credentials.project_id, credentials=credentials
    )
    df = pd.concat([df, chunk], ignore_index=True)

    df = permission_df_formatting(df, context)

    df_final = permissions_aggregate_values_calculation(df, context)

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    if context["prev_ds"] is None:
        base_fields = [
            {"name": "data_wplywu_wniosku_do_urzedu", "type": "TIMESTAMP"},
            {"name": "terc", "type": "STRING"},
        ]
        column_names_fields = [
            {"name": column_name, "type": "INTEGER"}
            for column_name in df_final.columns.tolist()
        ]
        schema_fields = base_fields + column_names_fields
        table_ref = client.dataset("permissions_dataset").table(
            "permissions_aggregates"
        )
        partitioning_config = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="data_wplywu_wniosku_do_urzedu",
        )
        table = bigquery.Table(table_ref, schema=schema_fields)
        table.time_partitioning = partitioning_config
        client.create_table(table)

    pandas_gbq.to_gbq(
        df_final,
        "permissions-417120.permissions_dataset.permissions_aggregates",
        project_id=credentials.project_id,
        if_exists="append",
        credentials=credentials,
        location="US",
    )


with DAG(
    default_args=default_args,
    dag_id="permissions_dag",
    description="Downloading, uploading, calculating aggregates of bulding permissions",
    schedule="@monthly",
    max_active_runs=1,
    start_date=datetime(2018, 1, 1),
) as dag:

    permissions_to_gbq = PythonOperator(
        task_id="permissions_to_gbq",
        provide_context=True,
        python_callable=permissions_to_gbq,
        dag=dag,
    )

    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id="permissions_dataset",
        table_id="permissions",
        schema_fields=[
            {"name": "numer_ewidencyjny_system", "type": "STRING", "mode": "REQUIRED"},
            {"name": "numer_ewidencyjny_urzad", "type": "STRING"},
            {"name": "data_wplywu_wniosku_do_urzedu", "type": "TIMESTAMP"},
            {"name": "nazwa_organu", "type": "STRING"},
            {"name": "wojewodztwo_objekt", "type": "STRING"},
            {"name": "obiekt_kod_pocztowy", "type": "STRING"},
            {"name": "miasto", "type": "STRING"},
            {"name": "terc", "type": "STRING"},
            {"name": "cecha", "type": "STRING"},
            {"name": "cecha_1", "type": "STRING"},
            {"name": "ulica", "type": "STRING"},
            {"name": "ulica_dalej", "type": "STRING"},
            {"name": "nr_domu", "type": "STRING"},
            {"name": "kategoria", "type": "STRING"},
            {"name": "nazwa_zam_budowlanego", "type": "STRING"},
            {"name": "rodzaj_zam_budowlanego", "type": "STRING"},
            {"name": "kubatura", "type": "STRING"},
            {"name": "stan", "type": "STRING"},
            {"name": "jednostki_numer", "type": "STRING"},
            {"name": "obreb_numer", "type": "STRING"},
            {"name": "numer_dzialki", "type": "STRING"},
            {"name": "numer_arkusza_dzialki", "type": "STRING"},
            {"name": "nazwisko_projektanta", "type": "STRING"},
            {"name": "imie_projektanta", "type": "STRING"},
            {"name": "projektant_numer_uprawnien", "type": "STRING"},
            {"name": "projektant_pozostali", "type": "STRING"},
        ],
        time_partitioning={"type": "MONTH", "field": "data_wplywu_wniosku_do_urzedu"},
        cluster_fields=["terc", "rodzaj_zam_budowlanego", "kategoria"],
        dag=dag,
    )

    permission_download = PythonOperator(
        task_id="building_permission_download",
        provide_context=True,
        python_callable=permission_download,
        dag=dag,
    )

    data_validation = PythonOperator(
        task_id="data_validation",
        provide_context=True,
        python_callable=data_validation,
        dag=dag,
    )

    aggregates_calculation = PythonOperator(
        task_id="building_permissions_aggregates",
        provide_context=True,
        python_callable=aggregates_calculation,
        dag=dag,
    )

    send_email = PythonOperator(
        task_id="send_email",
        provide_context=True,
        python_callable=send_email,
        op_kwargs={
            "to": "p.kuziola686@gmail.com",
            "subject": f"Database updated - {datetime.today().date()}",
            "files": ["/opt/airflow/validation_report.html"],
            "html_content": "<p>The data upload process has completed successfully.</p>",
        },
        dag=dag,
    )

    (
        create_table_task        
        >> data_validation
        >> permissions_to_gbq
        >> aggregates_calculation
        >> send_email
    )
