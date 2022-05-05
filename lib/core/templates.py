from base64 import b64encode
from hashlib import sha256, sha1, md5
from typing import Dict, Optional, Tuple
from functools import lru_cache
import re
from ipaddress import ip_address, ip_network
from tld import get_tld, get_fld
from .configs import Target
from dnslib import DNSRecord
from datetime import datetime
__all__ = ['create_result_template', 'unpack_packet',
           'create_error_template', 'make_document_from_response', 'validate_domain']

CONST_LRU_CACHE = 100000


def unpack_packet(payload: bytes) -> str:
    data = DNSRecord.parse(payload)
    return data


@lru_cache(maxsize=CONST_LRU_CACHE)
def ip_address_to_int(ipaddress_value: str) -> Optional[int]:
    try:
        return int(ip_address(ipaddress_value))
    except:
        pass


VALID_FQDN_REGEX = re.compile(r'(?=^.{4,253}$)(^((?!-)[*a-z0-9-_]{1,63}(?<!-)\.)+[a-z0-9-]{2,63}$)', re.IGNORECASE)


@lru_cache(maxsize=CONST_LRU_CACHE)
def validate_domain_1(domain: Optional[str]) -> bool:
    if domain:
        if VALID_FQDN_REGEX.match(domain):
            return True
    return False


@lru_cache(maxsize=CONST_LRU_CACHE)
def validate_domain(domain: Optional[str]) -> bool:
    # Validate the domain and, if invalid, see if it's IDN-encoded.
    if validate_domain_1(domain):
        return True
    else:
        try:
            domain_value = domain.encode("idna").decode("ascii")
        except UnicodeError:
            # pass
            return False
        else:
            return validate_domain_1(domain_value)


@lru_cache(maxsize=CONST_LRU_CACHE)
def wrap_get_fld(domain: str) -> Optional[Tuple[str, str, str, str]]:
    try:
        _domain = get_tld(domain, fix_protocol=True, as_object=True)
        if _domain.domain and _domain.tld:
            return _domain.subdomain, _domain.domain, _domain.tld, domain
    except:
        pass


@lru_cache(maxsize=CONST_LRU_CACHE)
def parse_hostname(_hostname: str, domain_field='hostname') -> Optional[Dict]:
    hostname = _hostname.lower().strip()
    if ip_address_to_int(hostname):
        return {'hostname': hostname}
    if validate_domain(_hostname):
        if _values_domain_record := wrap_get_fld(hostname):
            sub, name, tld, _ = _values_domain_record
            top = tld
            if '.' in tld:
                top = tld.split('.')[-1]
            return {'top': top, 'tld': tld, 'name': name,
                    'sub': sub, domain_field: hostname}


def create_result_template(target: Target) -> Dict:
    """
    Creates result dictionary skeleton
    """
    need_timestamp = int(datetime.now().timestamp())
    result = {'datetime': need_timestamp,
              'hostname': target.hostname,
              'nameserver': target.nameserver,
              'data': {'dns': {'status': 'unknown-error',
                              'protocol': 'dns',
                              'type': 'A',
                              'result': {'ipv4': [],
                                         'ip': [],
                                         'cname': [],
                                         'hostname': target.hostname,
                                         'nameserver': target.nameserver,
                                         'datetime': need_timestamp
                                         }
                               }
                       }
              }
    return result


def create_error_template(target: Target,
                          error_str: str,
                          description: str = '',
                          status: str = 'unknown-error',
                          ) -> Dict:
    """
    Creates skeleton of error result dictionary
    """
    need_timestamp = int(datetime.now().timestamp())
    result = {'datetime': need_timestamp,
              'hostname': target.hostname,
              'nameserver': target.nameserver,
              'data': {'dns': {'status': status,
                               'protocol': 'dns',
                               'type': 'A',
                               'error': error_str,
                               'description': description
                               }
                       }
                   }

    return result


def make_document_from_response(buffer: bytes, target: Target, addition_dict: Dict = None, protocol: str = '') -> Dict:
    req_keys = ['ipv4']
    data_struct = unpack_packet(buffer)
    result = create_result_template(target)
    try:
        if data_struct.rr:
            for value in data_struct.rr:

                if value.rtype == 1:
                    data = value.rdata.data
                    if len(data) == 4:
                        ip_int = sum(value * (256 ** (3 - i)) for i, value in enumerate(data))
                        result['data']['dns']['result']['ipv4'].append(ip_int)
                        ip_str = '.'.join(str(v) for v in data)
                        result['data']['dns']['result']['ip'].append(ip_str)
                elif value.rtype == 5:
                    data = value.rdata.label
                    data = '.'.join([v.decode() for v in data.label])
                    result['data']['dns']['result']['cname'].append(data)
        else:
            return create_error_template(target, '', status='not found')
    except Exception as e:
        return create_error_template(target,  type(e).__name__, type(e).__name__)
    if result['data']['dns']['result']['ipv4']:
        result['data']['dns']['status'] = 'success'
        if not result['data']['dns']['result']['cname']:
            result['data']['dns']['result'].pop('cname')
    else:
        return create_error_template(target, '')
    return result
