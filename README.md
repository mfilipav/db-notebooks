# db-notebooks
databricks notebooks CI/CD example, see https://docs.databricks.com/aws/en/notebooks/best-practices

# creating a new job and running it
Script usage:
```bash
python create_and_run_databricks_job.py \
  --databricks_instance https://dbc-c712b0f6-8b25.cloud.databricks.com \
  --databricks_access_token dap....a92\
  --notebook_path notebooks/covid_eda_modular \
  --data_path https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv \
  --table_version v7 \
  --user_email mfilipav@gmail.com \
  --git_url https://github.com/mfilipav/db-notebooks.git \
  --git_branch main
```
