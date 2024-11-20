from prefect import flow, get_run_logger, variables, task
from prefect.runtime import flow_run, deployment
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect.filesystems import GCS
from prefect.filesystems import GitHub
from google.cloud import storage
from pathlib import Path
from datetime import datetime, timedelta
import os
import io
import asyncio
import time
import UpdatingRMOLogs

@flow(log_prints=True)
def update_rmo_logs():
    _ = task(UpdatingRMOLogs.updating_RMO())

if __name__ == '__main__':
    with open(os.path.join(os.path.expanduser('~'), 'git_access_token.txt'), 'r') as f:
        git_token = f.read()
    block = GitHub(
        repository="https://github.com/uday-gadge/RMOLogs.git",
        access_token=git_token,
        reference='main'
    )
    block.save("git-rmo-logs", overwrite=True)
    
    git_storage = GitHub.load('git-rmo-logs')
    scheduled_deployment_rmo_logs = Deployment.build_from_flow(
        flow=update_rmo_logs,
        name="update_rmo_logs",
        storage=git_storage,
        schedule=(CronSchedule(cron="0 4 * * *", timezone="America/Denver")),
        work_queue_name="default",
        work_pool_name="my-process-pool"
    )

    scheduled_deployment_rmo_logs.apply()
