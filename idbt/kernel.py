import json
import pandas as pd

from ipykernel.kernelbase import Kernel
from idbt.server import DBTRPCServer
from idbt.client import DBTRPCClient


class DBTKernel(Kernel):
    def __init__(self, **kwargs):
        Kernel.__init__(self, **kwargs)
        self.server = DBTRPCServer()
        self.server.start()
        self.client = DBTRPCClient('http://localhost:8580')

    def do_clear(self):
        pass

    def do_apply(self, content, bufs, msg_id, reply_metadata):
        pass

    async def do_debug_request(self, msg):
        pass

    implementation = 'DBT'
    implementation_version = '1.0'
    language = 'sql'
    language_version = '0.1'
    language_info = {
        'name': 'sql',
        'mimetype': 'text/x-sql',
        'file_extension': '.sql',
    }
    banner = "DBT kernel"

    def do_execute(self, code, silent, store_history=True, user_expressions=None,
                   allow_stdin=False):
        r = self.client.run(code)
        rs = r.json()['result']['results'][0]
        sql = rs['compiled_sql']
        df = pd.DataFrame(rs['table']['rows'], columns=rs['table']['column_names'])
        if not silent:
            stream_content = {'name': 'stdout', 'text': json.dumps(sql)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            execute_content = {
                'data': {
                    'text/html': df.to_html()
                },
                'metadata': {},
            }
            self.send_response(self.iopub_socket, 'display_data', execute_content)

        return {
            'status': 'ok',
            'execution_count': self.execution_count,
            'payload': [],
            'user_expressions': {},
        }
