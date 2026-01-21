'''Lambda Function for initializing the MLOps process.'''

import json
import os
import logging
from datetime import datetime

from helper import processing_job

DATABASE_NAME = os.environ.get('DATABASE_NAME', 'default_db')
TABLE_NAME = os.environ.get('TABLE_NAME', 'default_table')
IMAGE_URI = os.environ.get("SAGEMAKER_IMAGE_URI", "")
ROLE_ARN = os.environ.get("SAGEMAKER_ROLE_ARN", "")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, _):
    """
    Lambda Handler para inicializar el proceso MLOps.
    Genera la query de extracción de datos basada en el periodo.
    """
    try:
        logger.info("Evento recibido: %s", json.dumps(event))

        date = datetime.now()
        timestamp = date.strftime('%Y%m%d%H%M%S')
        current_period = date.strftime("%Y-%m-%d")

        payload = event['Execution']['Input']
        periodo = payload.get('periodo', current_period)

        logger.info("Iniciando proceso para el periodo: %s", periodo)

        query_string = f"""
            SELECT *
            FROM {DATABASE_NAME}.{TABLE_NAME}
            WHERE fecha_proceso = '{periodo}'
        """

        logger.info("Query generada: %s", query_string)

        path_athena_result = f"s3://{S3_BUCKET_NAME}/athena-results/{timestamp}/"

        path_sagemaker_code = f"s3://{S3_BUCKET_NAME}/sagemaker/input/sagemaker.py"
        s3_output_uri = f"s3://{S3_BUCKET_NAME}/sagemaker/output/{timestamp}/"


        sagemaker = processing_job.make_request(
            code_uri=path_sagemaker_code,
            inputs=[
                processing_job.s3_input('data', path_athena_result)
            ],
            outputs=[
                processing_job.s3_output('result', s3_output_uri)
            ],
            job_name=f"ibk-mlops-processing-{timestamp}",
            image_uri=IMAGE_URI,
            role=ROLE_ARN
        )

        return {
            "DataSelection": {
                "QueryString": query_string,
                "ResultConfiguration": {
                    "OutputLocation": path_athena_result
                }
            },
            "MainJob": sagemaker,
            "timestamp": date.isoformat()
        }

    except Exception as e:
        logger.error("Error crítico en Initialize Lambda: %s", str(e))
        raise e
