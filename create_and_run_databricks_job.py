import argparse
import requests
import json
from time import sleep


def create_and_run_job(
    databricks_instance,
    databricks_access_token,
    notebook_path,
    data_path,
    table_version,
    user_email,
    git_url,
    git_branch,
    git_tag
):
    job_payload = {
        "name": "covid_report",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "run_notebook_tests",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "notebooks/run_unit_tests",
                    "source": "GIT"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "webhook_notifications": {}
            },
            {
                "task_key": "run_main_notebook",
                "depends_on": [
                    {
                        "task_key": "run_notebook_tests"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "data_path": data_path,
                        "table_version": table_version
                    },
                    "source": "GIT"
                },
                "timeout_seconds": 0,
                "email_notifications": {
                    "on_success": [user_email],
                    "on_failure": [user_email]
                },
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False
                },
                "webhook_notifications": {}
            }
        ],
        "git_source": {
            "git_url": git_url,
            "git_provider": "gitHub",
            "git_branch": git_branch
        },
        "queue": {
            "enabled": True
        },
        "performance_target": "STANDARD",
        "run_as": {
            "user_name": user_email
        }
    }

    if git_tag is not None:
        job_payload["git_source"]["git_tag"] = git_tag
        job_payload["git_source"].pop("git_branch", None)

    headers = {
        "Authorization": f"Bearer {databricks_access_token}",
        "Content-Type": "application/json"
    }

    # Create the job
    create_job_url = f"{databricks_instance}/api/2.1/jobs/create"
    response = requests.post(create_job_url, headers=headers, data=json.dumps(job_payload))
    response.raise_for_status()
    job_id = response.json()["job_id"]
    print(f"Job created with job_id: {job_id}")

    # Run the job
    sleep(1)    
    run_now_url = f"{databricks_instance}/api/2.1/jobs/run-now"
    run_payload = {"job_id": job_id}
    run_response = requests.post(run_now_url, headers=headers, data=json.dumps(run_payload))
    run_response.raise_for_status()
    run_id = run_response.json()["run_id"]
    print(f"Job run started with run_id: {run_id}")

def main():
    parser = argparse.ArgumentParser(description="Create and run a Databricks job.")
    parser.add_argument("--databricks_instance", type=str, default="https://dbc-c712b0f6-8b25.cloud.databricks.com")
    parser.add_argument("--databricks_access_token", type=str, default="dapi.......xxxx........2")
    parser.add_argument("--notebook_path", type=str, default="notebooks/covid_eda_modular", help="Path to the main notebook to run.")
    parser.add_argument("--data_path", type=str, default="https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv")
    parser.add_argument("--table_version", type=str, default="v6")
    parser.add_argument("--user_email", type=str, default="mfilipav@gmail.com")
    parser.add_argument("--git_url", type=str, default="https://github.com/mfilipav/db-notebooks.git")
    parser.add_argument("--git_branch", type=str, default="main")
    parser.add_argument("--git_tag", type=str, default=None)
    args = parser.parse_args()

    # Print all arguments and their values
    print("Start job creation with the following arg values:")
    for arg, value in vars(args).items():
        print(f"  {arg}: {value}")

    create_and_run_job(
        databricks_instance=args.databricks_instance,
        databricks_access_token=args.databricks_access_token,
        notebook_path=args.notebook_path,
        data_path=args.data_path,
        table_version=args.table_version,
        user_email=args.user_email,
        git_url=args.git_url,
        git_branch=args.git_branch,
        git_tag=args.git_tag
    )

if __name__ == "__main__":
    main()
    print("Done!")
