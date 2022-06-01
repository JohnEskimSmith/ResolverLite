import argparse
from os import path
from sys import stderr
from typing import Tuple
from lib.core import AppConfig, TargetConfig
from .net import is_ip
from itertools import cycle

__all__ = ['parse_args', 'parse_settings', 'QUERY_TYPES_ARE_SUPPORTED', 'abort']

QUERY_TYPES_ARE_SUPPORTED = ['A', 'ANY', 'CAA', 'CNAME', 'MX',  'NS', 'SOA', 'SRV', 'TXT']

def parse_args():
    parser = argparse.ArgumentParser(description='DNS resolver lite(asyncio)')
    parser.add_argument('-settings', type=str, help='path to file with settings (yaml)')
    parser.add_argument('--stdin', dest='input_stdin', action='store_true', help='Read targets from stdin')
    parser.add_argument('-t', '--targets', nargs='+', type=str, default='', dest='single_targets',
                        help='Single targets: ipv4, hostname, CIDRs')
    parser.add_argument('-q', '--query', type=str, default='A', dest='query',
                        help='single query: A, NS, TXT, MX ..., default: A')
    parser.add_argument('-r', '--nameservers', type=str, default='8.8.8.8,8.8.4.4,77.88.8.8,77.88.8.1', dest='nameservers',
                        help='nameservers as string with "," as split symbol, '
                             'default: 8.8.8.8,8.8.4.4')
    parser.add_argument('-f', '--input-file', dest='input_file', type=str, help='path to file with targets')
    parser.add_argument('-o', '--output-file', dest='output_file', type=str, help='path to file with results')
    parser.add_argument('-s', '--senders', dest='senders', type=int, default=1024,
                        help='Number of send coroutines to use (default: 1024)')
    parser.add_argument('--queue-sleep', dest='queue_sleep', type=int, default=1,
                        help='Sleep duration if the queue is full, default 1 sec. Queue size == senders')
    parser.add_argument('-timeout', '--timeout', dest='timeout', type=int, default=2,
                        help='Set timeout, seconds (default: 2)')
    parser.add_argument('--show-statistics', dest='statistics', action='store_true')
    parser.add_argument('--use-msgpack', dest='use_msgpack', action='store_true')
    parser.add_argument('--show-only-success', dest='show_only_success', action='store_true')
    return parser.parse_args()


# noinspection PyBroadException
def parse_settings(args: argparse.Namespace) -> Tuple[TargetConfig, AppConfig]:
    if args.settings:
        return parse_settings_file(args.settings)

    if not args.input_stdin and not args.input_file and not args.single_targets:
        print("""errors, set input source:
         --stdin read targets from stdin;
         -t,--targets set targets, see -h;
         -f,--input-file read from file with targets, see -h""")
        exit(1)

    input_file = None

    if args.input_file:
        input_file = args.input_file
        if not path.isfile(input_file):
            abort(f'ERROR: file not found: {input_file}')

    if not args.output_file:
        output_file, write_mode = '/dev/stdout', 'wb'
    else:
        output_file, write_mode = args.output_file, 'a'

    # endregion
    nameservers = []
    if args.nameservers:
        try:
            for nameserver in args.nameservers.split(','):
                if is_ip(nameserver.strip()):
                    nameservers.append(nameserver.strip())
        except:
            pass
    if not nameservers:
        abort(f'ERROR: not set nameservers: {args.nameservers}')

    query_types_are_supported = []
    if args.query:
        if args.query in QUERY_TYPES_ARE_SUPPORTED:
            query_types_are_supported = [args.query]
    if not query_types_are_supported:
        abort(f'ERROR: query type not supported: {args.query}')
    app_settings = AppConfig(**{
        'senders': args.senders,
        'queue_sleep': args.queue_sleep,
        'statistics': args.statistics,
        'input_file': input_file,
        'input_stdin': args.input_stdin,
        'single_targets': args.single_targets,
        'output_file': output_file,
        'write_mode': write_mode,
        'show_only_success': args.show_only_success,
        'nameservers': nameservers,
        'query_types_are_supported': query_types_are_supported,
        'timeout': args.timeout,
        'use_msgpack': args.use_msgpack
    })

    target_settings = TargetConfig(**{
        'nameservers': cycle(nameservers)
    })

    return target_settings, app_settings


def abort(message: str, exc: Exception = None, exit_code: int = 1):
    print(message, file=stderr)
    if exc:
        print(exc, file=stderr)
    exit(exit_code)


def parse_settings_file(file_path: str) -> Tuple[TargetConfig, AppConfig]:
    raise NotImplementedError('config read')
