import json
import os
import random
import sys
import time
from datetime import datetime

import sqlalchemy
from sqlalchemy import insert
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import Column


# Retrieve Job-defined env vars
TASK_INDEX = os.getenv("CLOUD_RUN_TASK_INDEX", 0)
# Retrieve User-defined env vars
SLEEP_MS = os.getenv("SLEEP_MS", 1000)

now = datetime.now

# Define main script
def main(sleep_ms=10000):
    """Program that simulates work using the sleep method and random failures.

    Args:
        sleep_ms: number of milliseconds to sleep
        fail_rate: rate of simulated errors
    """
    print(f"Starting Task #{TASK_INDEX}, Attempt at {now()}...")

    instance_connection_name = os.environ[
        "INSTANCE_CONNECTION_NAME"
    ]  # e.g. 'project:region:instance'
    db_user = os.environ["DB_USER"]  # e.g. 'my-database-user'
    db_pass = os.environ["DB_PASS"]  # e.g. 'my-database-password'
    db_name = os.environ["DB_NAME"]  # e.g. 'my-database'
    unix_socket_path = "/cloudsql/"+instance_connection_name # e.g. '/cloudsql/project:region:instance'
    engine = sqlalchemy.create_engine(
        # Equivalent URL:
        # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=<socket_path>/<cloud_sql_instance_name>
        sqlalchemy.engine.url.URL.create(
            drivername="mysql+pymysql",
            username=db_user,
            password=db_pass,
            database=db_name,
            query={"unix_socket": unix_socket_path},
        ),
        poolclass=NullPool
        # ...
    )
    conn = engine.connect() 
    metadata = sqlalchemy.MetaData() #extracting the metadata
    user_stat= sqlalchemy.Table(
        't_user_stat', metadata, 
        Column("stat_id", sqlalchemy.types.Integer, primary_key=True),
        Column("uid", sqlalchemy.types.Integer, nullable=False),
        Column("stat", sqlalchemy.types.String(30), nullable=False),
        Column("created_at", sqlalchemy.types.DateTime, nullable=True),
    ) #Table object
    stmt = (
        insert(user_stat).
        values(uid=random.randint(999000000, 999001000), stat='#'+TASK_INDEX, created_at=now())
    )
    Result = conn.execute(stmt)
    conn.commit()
    conn.close()
    engine.dispose()

    print(f"Completed Task #{TASK_INDEX}.")
    # Simulate work by waiting for a specific amount of time
    time.sleep(float(sleep_ms) / 1000)  # Convert to seconds

# Start script
if __name__ == "__main__":
    try:
        main(SLEEP_MS)
    except Exception as err:
        message = (
            f"Task #{TASK_INDEX}, " + f"Attempt at {now()} failed: {str(err)}"
        )

        print(json.dumps({"message": message, "severity": "ERROR"}))
        sys.exit(1)  # Retry Job Task by exiting the process