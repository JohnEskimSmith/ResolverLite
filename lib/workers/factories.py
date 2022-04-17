from typing import Iterator, Generator, Optional, List
from lib.core import Target, TargetConfig
from dnslib import DNSRecord

# noinspection PyArgumentList

def pack_packet_a(hostname: str) -> bytes:
    payload = DNSRecord.question(hostname)
    return bytes(payload.pack())


def create_target_dns_protocol(hostname: str, target_config: TargetConfig) -> Iterator[Target]:
    """
    На основании ip адреса и настроек возвращает через yield экземпляр Target.
    Каждый экземпляр Target содержит всю необходимую информацию(настройки и параметры) для функции worker.
    """
    kwargs = target_config.as_dict()
    kwargs['payload'] = pack_packet_a(hostname)
    yield Target(hostname=hostname, **kwargs)



def create_targets_dns_protocol(hosts: List[str], settings: TargetConfig) -> Generator[Target, None, None]:
    for _host in hosts:
        host = _host.lower().strip()
        for target in create_target_dns_protocol(host, settings):
            yield target