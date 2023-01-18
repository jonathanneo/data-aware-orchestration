from dagster import (
    repository,
    with_resources,
)

from dagster_airbyte import airbyte_resource
from dagster_dbt import dbt_cli_resource

from .db_io_manager import db_io_manager
from .utils.constants import DBT_CONFIG1, DBT_CONFIG2, POSTGRES_CONFIG

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    build_asset_reconciliation_sensor,
    define_asset_job,
)
from dagster_airbyte import (
    AirbyteManagedElementReconciler,
    airbyte_resource,
    AirbyteConnection,
    AirbyteSyncMode,
    load_assets_from_connections,
)
from dagster_airbyte.managed.generated.sources import GithubSource
from dagster_airbyte.managed.generated.destinations import (
    PostgresDestination,
)
from dagster_dbt import load_assets_from_dbt_project

import os
from .utils.constants import DBT_PROJECT_DIR1, DBT_PROJECT_DIR2

AIRBYTE_PERSONAL_GITHUB_TOKEN = os.environ.get(
    "AIRBYTE_PERSONAL_GITHUB_TOKEN", "please-set-your-token"
)
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "please-set-your-token")

airbyte_instance = airbyte_resource.configured(
    {
        "host": "localhost",
        "port": "8000",
        "username": "airbyte",
        "password": {"env": "AIRBYTE_PASSWORD"},
    }
)

gh_awesome_de_list_source = GithubSource(
    name="gh_awesome_de_list",
    credentials=GithubSource.PATCredentials(AIRBYTE_PERSONAL_GITHUB_TOKEN),
    start_date="2020-01-01T00:00:00Z",
    repository="dbt-labs/dbt-core", 
    branch="main",
    page_size_for_large_streams=100,
)

postgres_destination = PostgresDestination(
    name="postgres",
    host="localhost",
    port=5433,
    database="postgres",
    schema="public",
    username="postgres",
    password=POSTGRES_PASSWORD,
    ssl_mode=PostgresDestination.Disable(),
)

stargazer_connection = AirbyteConnection(
    name="fetch_stargazer",
    source=gh_awesome_de_list_source,
    destination=postgres_destination,
    stream_config={"stargazers": AirbyteSyncMode.incremental_append_dedup()},
    normalize_data=True,
)

airbyte_reconciler = AirbyteManagedElementReconciler(
    airbyte=airbyte_instance,
    connections=[stargazer_connection],
)

# load airbyte connection from above pythonic definitions
airbyte_assets = load_assets_from_connections(
    airbyte=airbyte_instance,
    connections=[stargazer_connection],
    key_prefix=["postgres"],
)

# preparing assets bassed on existing dbt project
dbt_assets_1 = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR1, dbt_resource_key="dbt1", io_manager_key="db_io_manager1", exclude="source:*"
)

dbt_assets_2 = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR2, dbt_resource_key="dbt2", io_manager_key="db_io_manager2"
)

update_sensor = build_asset_reconciliation_sensor(
    name="update_sensor", asset_selection=AssetSelection.all() 
)

my_job = define_asset_job(name="my_job", selection=AssetSelection.keys("postgres/stargazers", "postgres/stargazers_user"))

my_job_schedule = ScheduleDefinition(
    name="my_job_schedule", job=my_job, cron_schedule="*/30 * * * *"
)

@repository
def assets_modern_data_stack():
    return [
        airbyte_assets,
        with_resources(
            dbt_assets_1,  
            resource_defs={
                "dbt1": dbt_cli_resource.configured(DBT_CONFIG1),
                "db_io_manager1": db_io_manager.configured(POSTGRES_CONFIG),
            },
        ),
        with_resources(
            dbt_assets_2,  
            resource_defs={
                "dbt2": dbt_cli_resource.configured(DBT_CONFIG2),
                "db_io_manager2": db_io_manager.configured(POSTGRES_CONFIG),
            },
        ),
        update_sensor,
        my_job,
        my_job_schedule
    ]
