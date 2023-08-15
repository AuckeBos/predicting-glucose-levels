cd predicting_glucose_levels
prefect deploy --name daily-etl
prefect worker start -p docker-pool