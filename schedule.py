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
import mexl_dio_planning

@flow(log_prints=True)
def update_rmo_logs():
    _ = task(UpdatingRMOLogs.updating_RMO())

if __name__ == '__main__':
    with open(os.path.join(os.path.expanduser('~'), 'git_access_token.txt'), 'r') as f:
        git_token = f.read()
    block = GitHub(
        repository="https://github.com/anup-ctio/mexl_dio_planning.git",
        access_token=git_token,
        reference='main'
    )
    block.save("git-dio-planning", overwrite=True)
    
    git_storage = GitHub.load('git-dio-planning')
    
    scheduled_deployment_dio_dashboard = Deployment.build_from_flow(
        flow=update_dio_dashboard,
        name="mexl_dio_planning",
        storage=git_storage,
        schedule=(CronSchedule(cron="0 7 * * 1", timezone="America/Denver")),
        work_queue_name="default",
        work_pool_name="my-process-pool"
    )

    scheduled_deployment_dio_dashboard.apply()