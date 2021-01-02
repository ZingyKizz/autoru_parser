import urllib3
import collections
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def session_with_retries(session, retries=5, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def check_keys(d, keys=None):
    if not isinstance(d, collections.Mapping):
        raise TypeError('d argument should be a dict-like')
    if keys is not None:
        if not isinstance(keys, collections.Iterable):
            raise TypeError('keys argument should be an iterable')
        for key in keys:
            if key not in d.keys():
                raise ValueError(f'Unexpected result, {key} key is missing')


def response_post_json_with_retries(session, url, json, headers, verify=False, retries=5, keys=None):
    r = 0
    while r < retries:
        try:
            response = session.post(url, json=json, headers=headers, verify=verify)
            response_json = response.json()
        except ValueError:
            r += 1
            continue
        try:
            check_keys(d=response_json, keys=keys)
        except (ValueError, TypeError):
            r += 1
            continue
        return response_json
    raise ConnectionError('Max retries exceeded')