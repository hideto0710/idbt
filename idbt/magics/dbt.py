import pandas as pd
import IPython

from IPython.core import magic_arguments
from IPython.core.magic import Magics, magics_class, cell_magic

from idbt.client import DBTRPCClient


@magics_class
class DbtMagics(Magics):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = DBTRPCClient('http://localhost:8580')

    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument(
        "command",
        type=str,
        default=[""],
        nargs="*",
        help="Commands to execute."
    )
    @magic_arguments.argument(
        "--params",
        nargs="+",
        default=None,
        help="Parameters to request dbt rpc.",
    )
    @magic_arguments.argument(
        "--dest_var",
        default=None,
        help="If provided, save the output to this variable instead of displaying it.",
    )
    @magic_arguments.argument(
        "--verbose",
        action="store_true",
        default=False,
        help="If set, print verbose output.",
    )
    def dbt(self, line, cell):
        args = magic_arguments.parse_argstring(self.dbt, line)
        subcommand = args.command[0].lower()
        if subcommand == "run":
            result = self._run(cell)
            if args.dest_var:
                IPython.get_ipython().push({args.dest_var: result})
                return
            return result
        return args.verbose

    def _run(self, sql) -> pd.DataFrame:
        r = self.client.run(sql)
        rs = r.json()['result']['results'][0]
        return pd.DataFrame(rs['table']['rows'], columns=rs['table']['column_names'])
