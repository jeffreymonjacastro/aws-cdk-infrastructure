''' Utilidades para un processing job '''
from typing import Optional

def make_request(code_uri: str,
                 inputs: list,
                 outputs: list,
                 job_name: str,
                 image_uri: str,
                 role: str,
                 arguments: Optional[list] = None,
                 environment: Optional[dict] = None,
                 instance_type: str = 'ml.t3.medium', # 4 GB RAM & 2 vCPU
                 instance_count: int = 1,
                 volume_size_in_gb: int = 10):
    '''
    Crea el formato para solicitar un processing job.
    ### Parametros
    - code_uri: Ruta en S3 del script de python.
    - inputs: Entradas que se van a subir de S3 hacia el processing job.
    - outputs: Salidas que se van a subir del processing job hacia S3.
    - job_name: Nombre del job (no puede tener mas de 63 caracteres).
    - image_uri: ARN de la imagen en ECR que va a usar el processing job.
    - role: ARN del rol en IAM que va a usar el processing job.
    - arguments: Argumentos que va a recibir el script de python.
    - environment: Variables de entorno del processing job.
    - instance_type: Tipo de instancia de EC2 que va a usar el processing job.
    - instance_count: Cantidad de instancias que se van a desplegar.
    - volume_size_in_gb: Tamano del disco duro del processing job.
    ### Retorna
    - request: Formato para solicitar un processing job.
    '''

    job_name_max_length = 63
    if len(job_name) > job_name_max_length:
        raise ValueError(f"Value '{job_name}' violates max size of {job_name_max_length}")

    inputs = [i for i in inputs if i is not None]
    inputs.append(s3_input(input_name='code', s3_uri=code_uri))
    code_filename = code_uri.rsplit('/', maxsplit=1)[1]

    request = {
        'AppSpecification': {
            'ImageUri': image_uri,
            'ContainerEntrypoint': ['python3', f'/opt/ml/processing/input/code/{code_filename}'],
        },
        'ProcessingInputs': inputs,
        'ProcessingJobName': job_name,
        'ProcessingOutputConfig': {
            'Outputs': outputs,
        },
        'ProcessingResources': {
            'ClusterConfig': {
                'InstanceType': instance_type,
                'InstanceCount': instance_count,
                'VolumeSizeInGB': volume_size_in_gb,
            }
        },
        'RoleArn': role,
    }

    if arguments:
        request['AppSpecification']['ContainerArguments'] = arguments
    if environment:
        request['Environment'] = environment

    return request


def s3_input(input_name: str,
             s3_uri: str,
             s3_data_type: str = 'S3Prefix',
             s3_input_mode: str = 'File'):
    '''
    Crea el formato para la entrada de un processing job.
    ### Parametros
    - input_name: Nombre de la entrada.
    - s3_uri: Ruta de S3 de la entrada.
    - s3_data_type: Tipo de la ruta de entrada.
    - s3_input_mode: Tipo de contenido de la entrada.
    ### Retorna
    - dict_in: Formato para la entrada de un processing job.
    '''

    dict_in = None
    if s3_uri is not None:
        dict_in = {
            'InputName': input_name,
            'S3Input': {
                'S3Uri': s3_uri,
                'LocalPath': f'/opt/ml/processing/input/{input_name}',
                'S3DataType': s3_data_type,
                'S3InputMode': s3_input_mode
            }
        }

    return dict_in


def s3_output(output_name: str,
              s3_uri: str,
              s3_upload_mode: str = 'EndOfJob'):
    '''
    Crea el formato para la salida de un processing job.
    ### Parametros
    - output_name: Nombre de la salida.
    - s3_uri: Ruta de S3 de la salida.
    - s3_upload_mode: Tipo de subida de la salida.
    ### Retorna
    - dict_out: Formato para la salida de un processing job.
    '''

    dict_out = None
    if s3_uri is not None:
        dict_out = {
            'OutputName': output_name,
            'S3Output': {
                'S3Uri': s3_uri,
                'LocalPath': f'/opt/ml/processing/output/{output_name}',
                'S3UploadMode': s3_upload_mode
            }
        }

    return dict_out
