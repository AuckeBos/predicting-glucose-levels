nohup prefect server start &
# Await server start for 10 seconds
sleep 5
prefect work-pool create --type process default-pool
prefect worker start -p default-pool