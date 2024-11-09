# What's this?
- Airflow exercises.

# Environment
- Ubuntu 22.04 Server, 5.15.0-124-generic
- Docker version 27.3.1, Compose v2

# Setup
- Run ./setup.sh

# Troubleshooting
- Punch your monitor.
- Constant webserver sync worker sigkill
    - Maybe try reducing worker numbers or using gevent worker.
    - 1. `docker network inspect bridge` and get the container subnet address.
    - 2. sudo ufw allow from [subnet_address] to any port 5432
    - If it still don't work, dunno what'll do.
- No such python package error:
    - Try installing it on the worker container as well as the scheduler container.
    - Or just add the package to the `_pip_additional_requirements` in the compose yaml.
- Don't want to use them fudgin S3!!!
    - sql to csv to sftp >  to redshift dag
    - you can do this with two dags with a trigger
# Todo
- [ ] Add slack notifier

# Sticky
- Full refresh:
    - Deleeeeeeeeeeeete
    - Inseeeeeeeeeeeeeert
- Incremental update:
    - Copy from existing table to temp
    - Extract from source and insert it to temp also
    - Comb thru it using rownumber for latest records for each identifiers
    - Clean up the existing table
    - Push the filtered table to the existing table.
- When performing backfill:
    - source table must have created, modified, deleted fields
    - execution_date is used for incremental updates
    - catchup set to True
    - start_date/end_date -> backfill period
    - account for execution date and idempotency.
- Best practice
    - Don't underestimate the importance of pipeline meta data cataloguing
    - Which includes ownership and data lineage
    - Data QC: I/O data
    - Full refresh is preferred up until you hit the overhead
    - If it can't be helped, consider backfill incremental update
    - Cleanup (data, table, dag)