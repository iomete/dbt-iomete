import time
from enum import Enum

from typing import Dict, Any

import dbt.exceptions
from dbt.adapters.base import PythonJobHelper
from dbt.adapters.iomete import SparkCredentials
from dbt.events import AdapterLogger

from iomete_sdk.spark import SparkJobApiClient

POLLING_PERIOD_SECONDS = 10
DEFAULT_TIMEOUT = 60 * 60 * 24

logger = AdapterLogger("iomete")


class IometeSparkJobHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credential: SparkCredentials) -> None:
        self.parsed_model = parsed_model
        self.alias = parsed_model["alias"]
        self.schema = parsed_model["schema"]
        self.protocol = 'https://' if credential.https else 'http://'
        self.iom_client = SparkJobApiClient(
            host=f"{self.protocol}{credential.host}:{credential.port}",
            api_key=credential.token,
            domain=credential.domain
        )

    @property
    def job_id(self) -> str:
        spark_job_id = self.parsed_model["config"].get("spark_job_id")
        if not spark_job_id:
            raise ValueError("spark_job_id is required for submitting python models.")
        return spark_job_id

    def submit(self, compiled_code: str) -> Any:
        config_overrides = self.parsed_model["config"].get("spark_job_overrides", {})

        run_response = self._submit_job_run(job_id=self.job_id, payload={
            "pythonScript": compiled_code,
            "arguments": config_overrides.get("arguments", None),
            "envVars": config_overrides.get("envVars", None),
            "sparkConf": config_overrides.get("sparkConf", None),
        })
        logger.info(f"Spark job ({self.job_id}) triggered with run_id: {run_response['id']}")

        self._monitor_state(job_id=self.job_id, run_id=run_response["id"])

    def _create_job(self, payload):
        response = self.iom_client.create_job(payload=payload)
        return response

    def _submit_job_run(self, job_id, payload):
        response = self.iom_client.submit_job_run(job_id=job_id, payload=payload)
        return response

    def _get_job_run(self, job_id, run_id):
        response = self.iom_client.get_job_run_by_id(job_id=job_id, run_id=run_id)
        return response

    def _monitor_state(self, job_id, run_id):
        logger.info(f"Spark job submitted with job_id: {job_id}")

        while True:
            app = self._get_job_run(job_id, run_id)
            app_state = _get_state_from_app(app)
            if app_state.is_final:
                if app_state.is_successful:
                    logger.info(f"{job_id} completed successfully.")
                    return
                else:
                    error_message = "Job {j} failed with terminal state: {s}".format(
                        j=job_id, s=app_state.value
                    )
                    raise dbt.exceptions.DbtRuntimeError(error_message)
            else:
                logger.info("{} in app state: {}", job_id, app_state.value)
                logger.info("Sleeping for {} seconds.", POLLING_PERIOD_SECONDS)
                time.sleep(POLLING_PERIOD_SECONDS)


def _get_state_from_app(app):
    return ApplicationStateType(app.get("driverStatus", ""))


class ApplicationStateType(Enum):
    EmptyState = "ENQUEUED"
    DeployingState = "SUBMITTED"
    RunningState = "RUNNING"
    CompletedState = "COMPLETED"
    FailedState = "FAILED"
    AbortedState = "ABORTED"
    AbortingState = "ABORTING"
    ExecutorState = map(
        {"RUNNING": 1},
        {"PENDING": 1},
    )

    @property
    def is_final(self) -> bool:
        return self in [
            ApplicationStateType.CompletedState,
            ApplicationStateType.FailedState,
            ApplicationStateType.AbortedState,
        ]

    @property
    def is_successful(self) -> bool:
        return self == ApplicationStateType.CompletedState
