'''Stack for a test CDK application'''

import os
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda,
    aws_stepfunctions,
    aws_sns,
    aws_sns_subscriptions as sub,
    aws_iam,
)
from constructs import Construct

EMAIL = os.getenv("NOTIFICATION_EMAIL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
SAGEMAKER_IMAGE_URI = os.getenv("SAGEMAKER_IMAGE_URI", "")
SAGEMAKER_ROLE_ARN = os.getenv("SAGEMAKER_ROLE_ARN", "")
DATABASE_NAME = os.getenv("DATABASE_NAME", "")
TABLE_NAME = os.getenv("TABLE_NAME", "")

class MainStack(Stack):
    '''Stack for a test CDK application for Interbank MLOps'''

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ==========================================
        # 1. NOTIFICACIONES Y SEGURIDAD
        # ==========================================

        topic = aws_sns.Topic(self, "PipelineNotificationTopic",
            display_name="IBK MLOps Notifications"
        )
        
        if EMAIL:
            topic.add_subscription(
                sub.EmailSubscription(EMAIL) # type: ignore[arg-type]
            )

        # ==========================================
        # 2. CÓMPUTO (LAMBDA)
        # ==========================================

        path_lambda_code = "stacks/lambda_initialize"

        environment={
            "DATABASE_NAME": DATABASE_NAME,
            "TABLE_NAME": TABLE_NAME,
            "S3_BUCKET_NAME": S3_BUCKET_NAME,
            "SAGEMAKER_IMAGE_URI": SAGEMAKER_IMAGE_URI,
            "SAGEMAKER_ROLE_ARN": SAGEMAKER_ROLE_ARN
        }

        init_lambda = aws_lambda.Function(
            self,
            id="InitializeLambda",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            function_name="ibk_mlops_initialize_lambda",
            handler="lambda_function.handler",
            code=aws_lambda.Code.from_asset(path_lambda_code),
            timeout=Duration.seconds(30),
            environment=environment
        )

        init_lambda.add_to_role_policy(aws_iam.PolicyStatement(
            actions=["athena:StartQueryExecution", "athena:GetQueryExecution", "s3:ListBucket"],
            resources=["*"]
        ))

        # ==========================================
        # 3. STATE MACHINE DESDE ARCHIVO ASL (.json)
        # ==========================================

        path_sm = "stacks/step_function/pipeline.asl.json"
        sm_body = aws_stepfunctions.DefinitionBody.from_file(path_sm)

        substitutions={
            "lambda_initialize_arn": init_lambda.function_arn,
            "sns_topic_arn": topic.topic_arn
        }

        state_machine = aws_stepfunctions.StateMachine(
            self,
            "IBK_MLOps_StateMachine",
            definition_body=sm_body,
            definition_substitutions=substitutions,
            state_machine_name="ibk_mlops_pipeline_state_machine",
            timeout=Duration.minutes(10)
        )

        # Permisos para invocar la Lambda de inicialización y preparación
        state_machine.add_to_role_policy(aws_iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[init_lambda.function_arn]
        ))

        # Permisos para Athena (StartQueryExecution.sync)
        state_machine.add_to_role_policy(aws_iam.PolicyStatement(
            actions=[
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:StopQueryExecution",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:PutObject",
                "glue:GetTable",
                "glue:GetPartition"
            ],
            resources=["*"]
        ))

        # Permisos para SageMaker (CreateProcessingJob.sync)
        state_machine.add_to_role_policy(aws_iam.PolicyStatement(
            actions=[
                "sagemaker:CreateProcessingJob",
                "sagemaker:DescribeProcessingJob",
                "sagemaker:StopProcessingJob",
                "sagemaker:ListTags",
                "sagemaker:AddTags",
                "events:PutTargets",
                "events:PutRule",
                "events:DescribeRule"
            ],
            resources=["*"]
        ))

        # IMPORTANTE: Permiso para pasar roles (PassRole) a SageMaker
        state_machine.add_to_role_policy(aws_iam.PolicyStatement(
            actions=["iam:PassRole"],
            resources=["*"],
            conditions={"StringEquals": {"iam:PassedToService": "sagemaker.amazonaws.com"}}
        ))

        state_machine.add_to_role_policy(aws_iam.PolicyStatement(
            actions=["sns:Publish"],
            resources=[topic.topic_arn]
        ))
