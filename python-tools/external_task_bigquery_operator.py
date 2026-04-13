from airflow.sensors.base import BaseSensorOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.context import Context

class ExternalTaskBigQuerySensor(BaseSensorOperator):
    """
    A custom sensor that waits for an external task to complete,
    and then executes a BigQuery job.
    Supports poke and reschedule modes to save worker slots.
    """
    def __init__(
        self,
        external_dag_id: str,
        external_task_id: str,
        configuration: dict,
        deferrable: bool = False,
        poke_interval: int = 60,
        timeout: int = 600,
        mode: str = 'reschedule',
        *args, **kwargs
    ):
        # Pass sensor specific args to super (BaseSensorOperator)
        super().__init__(poke_interval=poke_interval, timeout=timeout, mode=mode, *args, **kwargs)
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.configuration = configuration
        self.deferrable = deferrable
        
    def poke(self, context: Context) -> bool:
        self.log.info(f"Poking for external task: {self.external_dag_id}.{self.external_task_id}")
        # Instantiate the sensor to reuse its poke logic
        sensor = ExternalTaskSensor(
            task_id=f"sensor_{self.task_id}",
            external_dag_id=self.external_dag_id,
            external_task_id=self.external_task_id,
        )
        return sensor.poke(context)
        
    def execute(self, context: Context):
        # 1. Run the sensor logic (handles poke/reschedule)
        # If mode='reschedule', this will raise AirflowRescheduleException
        # until poke() returns True.
        super().execute(context)
        
        # 2. Once the sensor succeeds (doesn't raise Reschedule), run the BQ job
        self.log.info("Sensor condition met. Proceeding to run BigQuery job.")
        bq_op = BigQueryInsertJobOperator(
            task_id=f"bq_{self.task_id}",
            configuration=self.configuration,
            deferrable=self.deferrable,
        )
        return bq_op.execute(context)
