from collections import namedtuple
from dataclasses import dataclass
from typing import Iterator, List


@dataclass(frozen=True)
class AppConfig:
    senders: int
    queue_sleep: int
    statistics: bool
    input_stdin: str
    single_targets: str
    input_file: str
    output_file: str
    write_mode: str
    show_only_success: bool
    nameservers: Iterator
    query_types_are_supported: List[str]
    timeout: int


@dataclass(frozen=True)
class TargetConfig:
    nameservers: Iterator

    def as_dict(self):
        nameserver = next(self.nameservers)
        return {'nameserver': nameserver}


Target = namedtuple('Target', ['hostname', 'nameserver', 'payload'])
