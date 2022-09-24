import base64
import requests
import time
import uuid

from requests import Response


class DBTRPCClient:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def _post(self, method: str, params: object) -> Response:
        request_id = str(uuid.uuid4())
        payload = {
            'method': method,
            'params': params,
            'id': request_id,
            'jsonrpc': '2.0',
        }
        return requests.post('{}/jsonrpc'.format(self.endpoint), json=payload)

    def _poll(self, request_token: str) -> Response:
        step = 10
        max_attempts = 60
        attempts = 0
        while True:
            attempts += 1
            res = self._post('poll', {
                'request_token': request_token,
            })
            if attempts >= max_attempts:
                return res
            if res.status_code != 200:
                return res
            if res.json()['result']['state'] == 'running':
                time.sleep(step)
                continue
            return res

    def run(self, sql: str) -> Response:
        name = str(uuid.uuid4())
        encoded = base64.b64encode(bytes(sql, 'utf-8')).decode('utf-8')
        res = self._post('run_sql', {
            'name': name,
            'sql': encoded,
        })
        return self._poll(res.json()['result']['request_token'])

    def list(self) -> Response:
        return self._post('list', {})
