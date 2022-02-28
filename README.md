To work with airflow, PosgreSQL Docker is used.
To run containers, please, follow the instructions:
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

Airflow is an ETL process schedular, a tool that allows to design, plan and monitor complex workflows.
Dags in airflow describe data processing process.
Tasks are the dag nodes, operations that applied to data.

Given in this project dag contains tasks that do the following.
1. Load files into corresponding tables in PostgreSQL.
2. Create a materialized view in PostgreSQL that solves some problem.
3. Send the table as a result of calling the created view to BigQuery using the appropriate API.
