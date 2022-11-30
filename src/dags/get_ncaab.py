import yaml
import pandas as pd
import pendulum
from datetime import datetime, timedelta
from lxml.etree import ParserError
import logging
import os

from sports.ncaab.boxscore import Boxscore, Boxscores
from sports.ncaab.teams import Teams
from sports.ncaab.schedule import Schedule

from airflow import DAG, macros
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup


tzinfo = pendulum.timezone("US/Central")

with open("config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)


def convert_date_string(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d")


def get_season(date):
    return (date + timedelta(days=192)).year


def get_base_filename(task_id, date, parent_dir=""):
    return os.path.join(parent_dir, f"{task_id}_{date}.parquet")


def get_base_dir(dag_id, season, downstream_task_id):
    return os.path.join(
        CONFIG["project"]["output_dir"], dag_id, downstream_task_id, season
    )


def branch_daily_backill(downstream_task_id, skip_task_id, **context):
    dag_date = convert_date_string(context["ds"])
    current_season = get_season(dag_date)
    base_dir = get_base_dir(
        context["dag"].dag_id, str(current_season), downstream_task_id
    )
    base_filename = get_base_filename(downstream_task_id, context["ds"], "advanced")
    if os.path.exists(os.path.join(base_dir, base_filename)):
        return skip_task_id
    return downstream_task_id


def branch_annual_backfill(downstream_task_id, skip_task_id, **context):
    max_season = get_season(datetime.today())
    dag_date = convert_date_string(context["ds"])
    current_season = get_season(dag_date)
    base_filename = get_base_filename(downstream_task_id, context["ds"], context["ds"])
    base_dir = get_base_dir(
        context["dag"].dag_id, str(current_season), downstream_task_id
    )
    if os.path.exists(os.path.join(base_dir, base_filename)):
        return skip_task_id
    if current_season == max_season:
        if os.path.exists(base_dir) and len(os.listdir(base_dir)) > 0:
            return skip_task_id
        return downstream_task_id
    if os.path.exists(base_dir) and len(os.listdir(base_dir)) > 0:
        return skip_task_id
    return downstream_task_id


def makedirs_if_exists(fullpath_filename):
    outdir = "/".join(fullpath_filename.split("/")[:-1])
    if not os.path.exists(outdir):
        os.makedirs(outdir)


with DAG(
    "ncaab",
    default_args={"owner": "airflow"},
    description="Get data from pysports-stats and write to disk",
    schedule_interval="0 11 * 11,12,1,2,3,4 *",
    start_date=datetime(2020, 9, 1, tzinfo=tzinfo),
    tags=["sportsipy"],
    catchup=True,
    concurrency=1,
    max_active_runs=1,
) as dag:

    boxscores_task_group = TaskGroup(group_id="boxscores_group", prefix_group_id=False)
    teams_task_group = TaskGroup(group_id="teams_group", prefix_group_id=False)

    def get_boxscores(**context):
        boxscore_date = convert_date_string(context["ds"])
        api_date_string = boxscore_date.strftime("%-m-%-d-%Y")
        current_season = get_season(boxscore_date)
        base_dir = get_base_dir(
            context["dag"].dag_id, str(current_season), context["task"].task_id
        )
        base_filename_basic = get_base_filename(
            context["task"].task_id, context["ds"], "basic"
        )
        base_filename_advanced = get_base_filename(
            context["task"].task_id, context["ds"], "advanced"
        )
        fullpath_filename_basic = os.path.join(base_dir, base_filename_basic)
        makedirs_if_exists(fullpath_filename_basic)
        fullpath_filename_advanced = os.path.join(base_dir, base_filename_advanced)
        makedirs_if_exists(fullpath_filename_advanced)

        boxscores = Boxscores(boxscore_date)
        if len(boxscores.games[api_date_string]) > 0:
            boxscores.dataframe.to_parquet(fullpath_filename_basic, index=False)
        print(boxscores.games)
        boxscored_detailed = []
        if (
            api_date_string in boxscores.games
            and len(boxscores.games[api_date_string]) > 0
        ):
            for game in boxscores.games[api_date_string]:
                if not game["non_di"]:
                    try:
                        boxscore = Boxscore(game["boxscore"])
                        # This triggers a value error for a bad boxscore url. This needs to be fixed
                        # https://github.com/bveber/pysports-stats/issues/17
                        logging.info(boxscore.__str__())
                        boxscored_detailed.append(boxscore.dataframe)
                    except (ParserError, IndexError, AttributeError) as e:
                        logging.error(e)
                        logging.error(f"Could not parse boxscore data for {game}")
                else:
                    logging.info(f"Skipping game {game} because it is not  D1 mathchup")
        else:
            logging.info(f"No games found for date: {api_date_string}")
            return

        if len(boxscored_detailed) > 0:
            boxscores_df = (
                pd.concat(boxscored_detailed)
                .reset_index()
                .rename(columns={"index": "boxscore_index"})
            )
            boxscores_df["season"] = current_season
            boxscores_df["date"] = pd.to_datetime(boxscores_df["date"])
            boxscores_df.to_parquet(fullpath_filename_advanced, index=False)

    def get_teams(**context):
        retrieve_date = convert_date_string(context["ds"])
        current_season = get_season(retrieve_date)
        teams = Teams(current_season)
        print(teams)
        base_filename = get_base_filename(
            context["task"].task_id, context["ds"], context["ds"]
        )
        base_dir = get_base_dir(
            context["dag"].dag_id, str(current_season), context["task"].task_id
        )
        fullpath_filename = os.path.join(base_dir, base_filename)
        makedirs_if_exists(fullpath_filename)
        teams.dataframes.to_parquet(fullpath_filename, index=False)

    # Tasks

    branch_boxscores = BranchPythonOperator(
        task_id="branch_boxscores",
        python_callable=branch_daily_backill,
        op_kwargs={"downstream_task_id": "boxscores", "skip_task_id": "boxscores_end"},
        task_group=boxscores_task_group,
    )

    boxscores_task = PythonOperator(
        task_id="boxscores",
        python_callable=get_boxscores,
        task_group=boxscores_task_group,
    )

    boxscores_end_task = DummyOperator(
        task_id="boxscores_end", task_group=boxscores_task_group
    )

    branch_teams = BranchPythonOperator(
        task_id="branch_teams",
        python_callable=branch_annual_backfill,
        op_kwargs={"downstream_task_id": "teams", "skip_task_id": "teams_end"},
        task_group=teams_task_group,
        trigger_rule="none_failed",
    )

    teams_task = PythonOperator(
        task_id="teams",
        python_callable=get_teams,
        task_group=teams_task_group,
        trigger_rule="none_failed",
    )

    teams_end_task = DummyOperator(
        task_id="teams_end", task_group=teams_task_group, trigger_rule="none_failed"
    )
    # This is to fix a deadlock issue. Upgrading
    end_task = DummyOperator(task_id="end", trigger_rule="none_failed")

    branch_boxscores >> [boxscores_task, boxscores_end_task] >> branch_teams
    branch_teams >> [teams_task, teams_end_task] >> end_task
