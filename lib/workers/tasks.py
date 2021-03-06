import abc
import asyncio
from abc import ABC
from asyncio import Queue
from base64 import b64encode
# noinspection PyUnresolvedReferences,PyProtectedMember
from ssl import _create_unverified_context as ssl_create_unverified_context
from typing import Optional, Callable, Any, Coroutine, Dict
import asyncio_dgram
from aioconsole import ainput
from aiofiles import open as aiofiles_open
from ujson import dumps as ujson_dumps
from msgpack import dumps as msgpack_dumps


from lib.core import validate_domain, create_error_template, make_document_from_response, Stats, AppConfig, \
    Target, TargetConfig
from lib.util import access_dot_path, is_ip, is_network, single_read, multi_read, \
    filter_bytes, write_to_file, write_to_stdout
from .factories import create_targets_dns_protocol

__all__ = ['QueueWorker', 'TargetReader', 'TargetFileReader', 'TargetStdinReader', 'TaskProducer', 'Executor',
           'OutputPrinter', 'TargetWorker', 'create_io_reader', 'get_async_writer']

STOP_SIGNAL = b'check for end'


class QueueWorker(metaclass=abc.ABCMeta):
    def __init__(self, stats: Optional[Stats] = None):
        self.stats = stats

    @abc.abstractmethod
    async def run(self):
        pass


class InputProducer:
    """
    Produces raw messages for workers
    """

    def __init__(self, stats: Stats, input_queue: Queue, target_conf: TargetConfig, send_limit: int, queue_sleep: int):
        self.stats = stats
        self.input_queue = input_queue
        self.target_conf = target_conf
        self.send_limit = send_limit
        self.queue_sleep = queue_sleep

    async def send(self, linein):
        if any([is_ip(linein), is_network(linein), validate_domain(linein)]):
            targets = create_targets_dns_protocol([linein], self.target_conf)  # generator
            if targets:
                for target in targets:
                    check_queue = True
                    while check_queue:
                        size_queue = self.input_queue.qsize()
                        if size_queue < self.send_limit:
                            if self.stats:
                                self.stats.count_input += 1
                            self.input_queue.put_nowait(target)
                            check_queue = False
                        else:
                            await asyncio.sleep(self.queue_sleep)

    async def send_stop(self):
        await self.input_queue.put(STOP_SIGNAL)


class TargetReader(QueueWorker, ABC):
    """
    Reads raw input messages from any source ans sends them to workers via producer
    """

    def __init__(self, stats: Stats, input_queue: Queue, producer: InputProducer):
        super().__init__(stats)
        self.input_queue = input_queue
        self.producer = producer


class TargetFileReader(TargetReader):
    """
    Reads raw input messages from text file
    """

    def __init__(self, stats: Stats, input_queue: Queue, producer: InputProducer, file_path: str):
        super().__init__(stats, input_queue, producer)
        self.file_path = file_path

    async def run(self):
        async with aiofiles_open(self.file_path, mode='rt') as f:
            async for line in f:
                linein = line.strip()
                await self.producer.send(linein)

        await self.producer.send_stop()


class TargetSingleReader(TargetReader):
    """
    Reads --target input messages from args
    """

    def __init__(self, stats: Stats, input_queue: Queue, producer: InputProducer, single_targets: str):
        super().__init__(stats, input_queue, producer)
        self.single_targets = single_targets

    async def run(self):
        for single_target in self.single_targets:
            linein = single_target.strip()
            if linein:
                await self.producer.send(linein)
        await self.producer.send_stop()


class TargetStdinReader(TargetReader):
    """
    Reads raw input messages from STDIN
    """

    async def run(self):
        """
        ?????????????????????? ???????????? aioconsole ?????????????? "????????????????????" ???????????? ???? stdin ????????????, ???????????????????????????? ??????????
        ?????????????????????? ?????? ip ?????????? ?????? ???????????? ?????????????? ?? ipv4
        ???? ???????????? ???????????? ?????????????????????? ?????????????????? Target, ?????????????? ???????????????????????? ?? ??????????????
        TODO: ???????????????????????? ???????? ???????????? - ?????? aioconsole ?????? aiofiles
        """
        while True:
            try:
                linein = (await ainput()).strip()
                await self.producer.send(linein)
            except EOFError:
                await self.producer.send_stop()
                break


class TaskProducer(QueueWorker):
    """
    Creates tasks for tasks queue
    """

    def __init__(self, stats: Stats, in_queue: Queue, tasks_queue: Queue, worker: 'TargetWorker'):
        super().__init__(stats)
        self.in_queue = in_queue
        self.tasks_queue = tasks_queue
        self.worker = worker

    async def run(self):
        while True:
            # wait for an item from the "start_application"
            target = await self.in_queue.get()
            if target == STOP_SIGNAL:
                await self.tasks_queue.put(STOP_SIGNAL)
                break
            if target:
                coro = self.worker.do(target)
                task = asyncio.create_task(coro)
                await self.tasks_queue.put(task)


class Executor(QueueWorker):
    """
    Gets tasks from tasks queue and launch execution for each of them
    """

    def __init__(self, stats: Stats, tasks_queue: Queue, out_queue: Queue):
        super().__init__(stats)
        self.tasks_queue = tasks_queue
        self.out_queue = out_queue

    async def run(self):
        while True:
            # wait for an item from the "start_application"
            task = await self.tasks_queue.get()
            if task == STOP_SIGNAL:
                await self.out_queue.put(STOP_SIGNAL)
                break
            if task:
                await task


class OutputPrinter(QueueWorker):
    """
    Takes results from results queue and put them to output
    """

    def __init__(self, output_file:str, stats: Stats, in_queue: Queue, io, async_writer) -> None:
        super().__init__(stats)
        self.in_queue = in_queue
        self.async_writer = async_writer
        self.io = io
        self.output_file = output_file

    async def run(self):
        while True:
            line = await self.in_queue.get()
            if line == STOP_SIGNAL:
                break
            if line:
                await self.async_writer(self.io, line)

        await asyncio.sleep(0.5)
        if self.stats:
            statistics = self.stats.dict()
            if self.output_file == '/dev/stdout':
                await self.io.write(ujson_dumps(statistics).encode('utf-8') + b'\n')
            else:
                async with aiofiles_open('/dev/stdout', mode='wb') as stats:
                    await stats.write(ujson_dumps(statistics).encode('utf-8') + b'\n')


def pack_dict_to_msgpack_string(value: Dict) -> str:
    result_msg: bytes = msgpack_dumps(value)
    return b64encode(result_msg).decode('ascii')


class TargetWorker:
    """
    send "payloads" to DNS servers
    """

    def __init__(self, stats: Stats, semaphore: asyncio.Semaphore, output_queue: asyncio.Queue,
                 success_only: bool, use_msgpack: bool = False):
        self.stats = stats
        self.semaphore = semaphore
        self.output_queue = output_queue
        self.success_only: bool = success_only
        self.function_pack: Callable = pack_dict_to_msgpack_string if use_msgpack else ujson_dumps

    async def send_result(self, result: Optional[Dict]):
        if result:
            try:
                success = access_dot_path(result, 'data.dns.status')
            except:
                success = 'unknown-error'

            if self.stats:
                if success == 'success':
                    self.stats.count_good += 1
                else:
                    self.stats.count_error += 1

            record = None
            if self.success_only:
                if success == 'success':
                    record = result
            else:
                record = result

            if record:
                record_out: str = self.function_pack(record)
                await self.output_queue.put(record_out)

    # noinspection PyBroadException
    async def do(self, target: Target):
        """
        ??????????????????????, ???????????????????????? ?????????????????????? ?? Target, ???????????????? ?? ?????????? ????????????, ?????????????????? ???????????????????? ?? ???????? dict
        """
        async with self.semaphore:
            result = None
            future_connection = asyncio_dgram.connect((target.nameserver, 53))
            try:
                stream = await asyncio.wait_for(future_connection, timeout=1.5)
            except:
                result = create_error_template(target, 'unknown')
            else:
                try:
                    await stream.send(target.payload)
                    future_connection = stream.recv()
                    try:
                        data, remote_addr = await asyncio.wait_for(future_connection, timeout=1.5)
                    except asyncio.TimeoutError:
                        await asyncio.sleep(0.005)
                        try:
                            stream.close()
                            del stream
                        except Exception as e:
                            pass
                        result = create_error_template(target, 'timeout')
                    else:
                        result = make_document_from_response(data, target, protocol='dns')
                        stream.close()
                except Exception as e:
                    await asyncio.sleep(0.005)
                    try:
                        stream.close()
                        del stream
                    except Exception as e:
                        pass
                    result = create_error_template(target, str(e))
            if result:
                await self.send_result(result)


def create_io_reader(stats: Stats, queue_input: Queue, target: TargetConfig, app_config: AppConfig) -> TargetReader:
    message_producer = InputProducer(stats, queue_input, target, app_config.senders - 1, app_config.queue_sleep)
    if app_config.input_stdin:
        return TargetStdinReader(stats, queue_input, message_producer)
    if app_config.single_targets:
        return TargetSingleReader(stats, queue_input, message_producer, app_config.single_targets)
    elif app_config.input_file:
        return TargetFileReader(stats, queue_input, message_producer, app_config.input_file)
    else:
        # TODO : rethink...
        print("""errors, set input source:
         --stdin read targets from stdin;
         -t,--targets set targets, see -h;
         -f,--input-file read from file with targets, see -h""")
        exit(1)


def get_async_writer(app_settings: AppConfig) -> Callable[[Any, str], Coroutine]:
    if app_settings.write_mode == 'a':
        return write_to_file
    return write_to_stdout
