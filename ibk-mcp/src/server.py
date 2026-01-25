''' Example MCP server for currency conversion tool '''

import os
import time
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import awswrangler as wr
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

load_dotenv()

DATABASE_NAME = os.getenv("DATABASE_NAME", "")
TABLE_NAME = os.getenv("TABLE_NAME", "")
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET", "")
CACHE_TABLE_NAME = os.getenv("CACHE_TABLE_NAME", "")

# Inicializar DynamoDB
dynamodb = boto3.resource("dynamodb")
cache_table = dynamodb.Table(CACHE_TABLE_NAME) if CACHE_TABLE_NAME else None  # type: ignore

# Configurar FastMCP con debug y dependencias explícitas para evitar conflictos
mcp = FastMCP("IBK-MCP-Server")

def get_cached_result(key: str):
    """Obtiene el resultado de la caché si existe"""
    if not cache_table:
        return None

    try:
        response = cache_table.get_item(Key={'name_table': key})
        if 'Item' in response:
            print(f"Cache hit for {key}")
            return response['Item'].get('data')
    except ClientError as e:
        print(f"Error reading cache: {e}")

    return None

def save_cached_result(key: str, data: str, ttl_seconds: int = 3600):
    """Guarda el resultado en la caché"""
    if not cache_table:
        return

    try:
        expiration = int(time.time()) + ttl_seconds
        cache_table.put_item(
            Item={
                'name_table': key,
                'data': data,
                'ttl': expiration
            }
        )
        print(f"Cache saved for {key}")
    except ClientError as e:
        print(f"Error saving cache: {e}")

def sql_query(query: str) -> pd.DataFrame:
    """
    Ejecuta una consulta SQL en Athena y devuelve el resultado como un DataFrame.
    Args:
        query (str): Consulta SQL a ejecutar.
    Returns:
        pd.DataFrame: DataFrame con los resultados de la consulta.
    """

    df_result = wr.athena.read_sql_query(
        sql=query,
        database=DATABASE_NAME,
        s3_output=S3_OUTPUT_BUCKET,
        ctas_approach=False
    )

    return df_result

@mcp.tool()
def get_data_by_period(periodo: str) -> str:
    """
    Obtiene datos de la tabla especificada para un período dado.
     Primero consulta la caché (DynamoDB), si no encuentra el dato, consulta Athena.
    Args:
        periodo (str): Período para filtrar los datos (formato 'YYYY-MM-DD').
    Returns:
        str: Resultados de la consulta en formato de cadena.
    """

    cache_key = f"period_{periodo}"

    # 1. Intentar obtener de caché
    cached_data = get_cached_result(cache_key)
    if cached_data:
        return cached_data

    # 2. Si no está en caché, consultar Athena
    query = f"""
    SELECT *
    FROM {DATABASE_NAME}.{TABLE_NAME}
    WHERE fecha_proceso = '{periodo}'
    """

    try:
        result = sql_query(query)
        result_str = result.to_string()

        # 3. Guardar en caché
        save_cached_result(cache_key, result_str)

        return result_str

    except Exception as e:
        return f"Error executing query: {str(e)}"


if __name__ == "__main__":
    mcp.run(transport="sse")
