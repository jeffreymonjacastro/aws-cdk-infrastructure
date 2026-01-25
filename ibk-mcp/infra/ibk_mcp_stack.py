''' Stack de AWS '''

import os
from aws_cdk import (
    Stack,
    aws_lambda,
    aws_iam,
    aws_dynamodb,
    Duration,
    RemovalPolicy,
    CfnOutput
)
from constructs import Construct

DATABASE_NAME = os.getenv("DATABASE_NAME", "")
TABLE_NAME = os.getenv("TABLE_NAME", "")
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET", "")

class IbkMcpStack(Stack):
    ''' Stack principal de la aplicación MCP consulting '''

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Crear Tabla DynamoDB para el Caché (Reemplazo de SQLite)
        cache_table = aws_dynamodb.Table(
            self, "McpDataCache",
            partition_key=aws_dynamodb.Attribute(
                name="name_table",
                type=aws_dynamodb.AttributeType.STRING
            ),
            time_to_live_attribute="ttl", # Opcional: para que el caché expire solo
            removal_policy=RemovalPolicy.DESTROY, # ¡Cuidado en PROD! En DEV está bien.
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST # Serverless real
        )

        environment={
            "CACHE_TABLE_NAME": cache_table.table_name,
            "DATABASE_NAME": DATABASE_NAME,
            "TABLE_NAME": TABLE_NAME,
            "S3_OUTPUT_BUCKET": S3_OUTPUT_BUCKET,
            "AWS_LWA_INVOKE_MODE": "response_stream"
        }

        # 2. Configuración de Layers (Capas) para dependencias
        wrangler_layer = aws_lambda.LayerVersion.from_layer_version_arn(
            self, "WranglerLayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSSDKPandas-Python311-Arm64:24"
        )

        # B. Lambda Web Adapter Layer (Permite correr el servidor web MCP en Lambda)
        adapter_layer = aws_lambda.LayerVersion.from_layer_version_arn(
            self, "AdapterLayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:753240598075:layer:LambdaAdapterLayerArm64:24"
        )

        # C. Local MCP Layer (Librerías ligeras instaladas localmente)
        mcp_layer = aws_lambda.LayerVersion(
            self, "McpLayer",
            code=aws_lambda.Code.from_asset("layers/mcp-layer"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_11],
            description="Capa con dependencias MCP, Uvicorn, FastAPI"
        )

        # 3. Definición de la Lambda (Zip en lugar de Docker)
        mcp_function = aws_lambda.Function(
            self, "McpLambda",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            code=aws_lambda.Code.from_asset("src"), 
            handler="run.sh",
            architecture=aws_lambda.Architecture.ARM_64,
            memory_size=1024,
            timeout=Duration.minutes(5),
            environment=environment,
            layers=[wrangler_layer, mcp_layer]
        )

        # 3. Asignación de Permisos (Principio de Menor Privilegio)

        # A. Permiso para usar la tabla de Caché (DynamoDB)
        cache_table.grant_read_write_data(mcp_function)

        # B. Permisos para Athena y Glue (Necesario para ver tablas)
        # Nota: En un entorno real de Interbank, restringiríamos los recursos ("*")
        mcp_function.add_to_role_policy(aws_iam.PolicyStatement(
            actions=[
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "glue:GetTable",       # Crucial: Athena usa Glue Catalog
                "glue:GetPartitions",
                "glue:GetDatabase"
            ],
            resources=["*"] # Idealmente: arn:aws:athena:region:account:workgroup/primary
        ))

        # C. Permisos para S3 (Athena guarda resultados aquí)
        # La Lambda necesita leer el resultado que Athena escribe.
        mcp_function.add_to_role_policy(aws_iam.PolicyStatement(
            actions=[
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject" # Athena necesita escribir los resultados
            ],
            resources=[
                f"arn:aws:s3:::{S3_OUTPUT_BUCKET}",
                f"arn:aws:s3:::{S3_OUTPUT_BUCKET}/*",
                "arn:aws:s3:::tu-bucket-donde-esta-la-data-original/*"
            ]
        ))

        # 4. Configurar Function URL
        # Esto genera un endpoint HTTPS público (o protegido con IAM)
        fn_url = mcp_function.add_function_url(
            auth_type=aws_lambda.FunctionUrlAuthType.NONE,
            cors=aws_lambda.FunctionUrlCorsOptions(
                allowed_origins=["*"],
                allowed_methods=[aws_lambda.HttpMethod.ALL],
                allowed_headers=["*"],
                allow_credentials=True,     # Requerido para SSE en algunos clientes
                max_age=Duration.days(1)
            )
        )

        # 5. Output del URL
        CfnOutput(
            self, "McpServerUrl",
            value=fn_url.url,
            description="URL del servidor MCP"
        )
