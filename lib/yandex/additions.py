from typing import Tuple, List, Dict, Optional
from os import environ as os_environ
from tempfile import NamedTemporaryFile
from base64 import decodebytes
from zlib import decompress
from uuid import uuid4
from itertools import cycle
from contextlib import AsyncExitStack
from aiobotocore.session import AioSession
from lib.util import is_ip
from lib.util import QUERY_TYPES_ARE_SUPPORTED, abort, access_dot_path
from lib.core import AppConfig, TargetConfig

__all__ = ['parse_args_env']

CONST_SPECIAL_PREFIX_BUCKET = 'destination_'


def unpack_targets_to_str(payload: str) -> Optional[List[str]]:
    try:
        bytes_record = payload.encode('utf-8')  # 1. данные приходят в base64
        zlib_record: bytes = decodebytes(bytes_record)  # 2. из base64 в bytes, которые являются запакованы zlib
        decompressed_record: bytes = decompress(zlib_record)  # 3. распаковка zlib
        massive_strings: str = decompressed_record.decode('utf-8')
        values: List[str] = massive_strings.split('\n')  # for example
        return values
    except:
        pass


def parse_sqs_message_yandex(event: Dict) -> Optional[str]:
    try:
        message = event['messages'][0]
        body_current_message = access_dot_path(message, 'details.message.body')
        records: Optional[List[str]] = unpack_targets_to_str(body_current_message)
        if records:
            tmp_file = NamedTemporaryFile(delete=False, mode='wt')
            tmp_file.write('\n'.join(records)+'\n')
            return tmp_file.name
    except Exception as exp:
        print(exp)


async def create_aws_client(session: AioSession, exit_stack: AsyncExitStack, auth_struct: Dict):
    # Create client and add cleanup
    client = await exit_stack.enter_async_context(session.create_client(**auth_struct))
    return client


def create_default_info_for_routes_bucket(settings_s3: Dict) -> Dict:
    endpoint = settings_s3['endpoint'].strip('/')
    dest, database, space = endpoint.split('/')
    try:
        _name_task = settings_s3['name']
    except:
        _name_task = space

    currentuuid = uuid4().hex
    s3_prefix_key = f'{database}/{space}/{_name_task}_uuid_{currentuuid}.gzip'
    return {'bucket': CONST_SPECIAL_PREFIX_BUCKET + dest,
            'key': s3_prefix_key}


async def parse_args_env(event: Dict) -> Tuple[TargetConfig, AppConfig, Dict, Optional[Dict]]:

    input_file: Optional[str] = parse_sqs_message_yandex(event)
    if not input_file:
        abort(f'ERROR: errors when creating input file(temp.)')

    default_nameservers = ['8.8.8.8', '8.8.4.4', '77.88.8.8', '77.88.8.1']
    nameservers = []
    if os_environ.get('nameservers'):
        _nameservers = os_environ.get('nameservers')
        try:
            for nameserver in _nameservers.split(','):
                if is_ip(nameserver.strip()):
                    nameservers.append(nameserver.strip())
        except:
            pass
    if not nameservers:
        nameservers = default_nameservers
    senders = 1024
    try:
        senders = int(os_environ.get('senders'))
    except:
        pass
    query_types_are_supported = []
    if query := os_environ.get('query'):
        if query in QUERY_TYPES_ARE_SUPPORTED:
            query_types_are_supported = [query]
    if not query_types_are_supported:
        abort(f'ERROR: query type not supported: {query}')

    output_file = f'/tmp/{uuid4().hex}.results'
    # region client s3
    s3_out_struct = {'service_name': 's3',
                     'region_name': os_environ.get('region_name', 'ru-east-1'),
                     'use_ssl': True,
                     'endpoint_url': os_environ['endpoint_url'],
                     'aws_secret_access_key': os_environ['aws_secret_access_key'],
                     'aws_access_key_id': os_environ['aws_access_key_id'],
                     'endpoint': '/mongo/dns/hosts',
                     'name': os_environ.get('name_task', 'simple')}

    keys = ['service_name', 'endpoint_url', 'region_name', 'aws_secret_access_key', 'aws_access_key_id', 'use_ssl']
    init_keys = {k: s3_out_struct.get(k) for k in keys if s3_out_struct.get(k)}

    _session = AioSession()
    exit_stack = AsyncExitStack()
    client_s3 = await create_aws_client(_session, exit_stack, auth_struct=init_keys)
    print('created Client for S3')

    s3 = dict()
    s3['init_keys'] = init_keys
    s3['client'] = client_s3
    s3['endpoint'] = s3_out_struct['endpoint']
    s3['about_bucket']: Dict = create_default_info_for_routes_bucket(s3)
    s3['output_file'] = output_file
    # endregion
    # region client sqs
    sqs_out_struct = {'service_name': 'sqs',
                      'region_name': os_environ.get('region_name_sqs', 'ru-east-1'),
                      'use_ssl': True,
                      'endpoint_url': os_environ.get('endpoint_url_sqs'),
                      'aws_secret_access_key': os_environ.get('aws_secret_access_key_sqs'),
                      'aws_access_key_id': os_environ.get('aws_access_key_id_sqs'),
                      'queue_url': os_environ.get('queuq_url_sqs')
                      }
    if sqs_out_struct['queue_url']:  # TODO: rewrite checking settings
        keys = ['service_name', 'endpoint_url', 'region_name', 'aws_secret_access_key', 'aws_access_key_id', 'use_ssl']
        init_keys = {k: sqs_out_struct.get(k) for k in keys if sqs_out_struct.get(k)}

        _session = AioSession()
        exit_stack = AsyncExitStack()
        client_sqs = await create_aws_client(_session, exit_stack, auth_struct=init_keys)
        print('created Client for SQS')
        sqs = dict()
        sqs['init_keys'] = init_keys
        sqs['client'] = client_sqs
        sqs['queue_url'] = sqs_out_struct['queue_url']
    else:
        sqs = None
        print('mode about SQS - not enabled')

    # endregion
    show_only_success = True if os_environ.get('show_only_success', '') == 'True' else False
    app_settings = AppConfig(**{
        'senders': senders,
        'queue_sleep': 1,
        'statistics': False,
        'input_file': input_file,
        'input_stdin': False,
        'single_targets': '',
        'output_file': output_file,
        'write_mode': 'a',
        'show_only_success': show_only_success,
        'nameservers': nameservers,
        'query_types_are_supported': query_types_are_supported,
        'timeout': 2,
        'use_msgpack': False
    })

    target_settings = TargetConfig(**{
        'nameservers': cycle(nameservers)
    })
    return target_settings, app_settings, s3, sqs