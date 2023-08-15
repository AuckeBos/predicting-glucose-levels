from logging import LoggerAdapter

from kink import di
from prefect import flow, get_run_logger, task

from predicting_glucose_levels.data.metadata import Metadata
from predicting_glucose_levels.data.transformation.transformer.transformers.base_transformer import (
    BaseTransformer,
)
from predicting_glucose_levels.data.transformation.transformer.transformers.glucose_measurements_transformer import (
    GlucoseMeasurementTransformer,
)


@flow(validate_parameters=False)
def etl():
    # Use dependency injection to inject the prefect logger
    di[LoggerAdapter] = get_run_logger()
    transformers = [GlucoseMeasurementTransformer]

    @task
    def transform_one(_cls):
        transformer = _cls()
        transformer.etl()

    for _cls in transformers:
        transform_one(_cls)
