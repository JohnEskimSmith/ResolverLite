# -*- coding: utf-8 -*-
__author__ = "SAI"
__status__ = "Dev"

import asyncio
import uvloop
from aiofiles import open as aiofiles_open
from os import unlink
from lib.workers import get_async_writer, create_io_reader, TargetReader, TaskProducer, Executor, OutputPrinter, \
    TargetWorker
from gzip import compress as gzip_compress
from lib.core import Stats
from lib.yandex import parse_args_env


async def main(event, context):
    target_settings, config, s3_config = await parse_args_env(event)
    queue_input = asyncio.Queue()
    queue_tasks = asyncio.Queue()
    queue_prints = asyncio.Queue()

    task_semaphore = asyncio.Semaphore(config.senders)
    statistics = Stats() if config.statistics else None

    async with aiofiles_open(config.output_file, mode=config.write_mode) as file_with_results:
        writer_coroutine = get_async_writer(config)

        target_worker = TargetWorker(statistics,
                                     task_semaphore,
                                     queue_prints,
                                     config.show_only_success,
                                     use_msgpack=config.use_msgpack)

        input_reader: TargetReader = create_io_reader(statistics, queue_input, target_settings, config)
        task_producer = TaskProducer(statistics, queue_input, queue_tasks, target_worker)
        executor = Executor(statistics, queue_tasks, queue_prints)
        printer = OutputPrinter(config.output_file, statistics, queue_prints, file_with_results, writer_coroutine)

        running_tasks = [asyncio.create_task(worker.run())
                         for worker in [input_reader, task_producer, executor, printer]]
        await asyncio.wait(running_tasks)

    # region send file to S3 bucket
    with open(config.output_file, 'rb') as outfile:
        data = outfile.read()
        data_packed = gzip_compress(data, compresslevel=4)
    client_s3 = s3_config['client']
    bucket = s3_config['about_bucket']['bucket']
    key_bucket = s3_config['about_bucket']['key']
    resp_from_s3 = await client_s3.put_object(Bucket=bucket,
                                              Key=key_bucket,
                                              Body=data_packed)
    try:
        http_status = resp_from_s3['ResponseMetadata']['HTTPStatusCode']
    except Exception as exp:
        http_status = 0
        print(exp)
    try:
        await s3_config['client'].close()
    except Exception as e:
        print(e)
        print('errors when closing S3 Client connection')
    # endregion
    # need delete tmp file
    try:
        unlink(config.input_file)
        unlink(config.output_file)
    except:
        pass
    return http_status


def handler(event, context):
    uvloop.install()
    s3_status = asyncio.run(main(event, context))
    return {'statusCode': 200,
            'body': s3_status}

