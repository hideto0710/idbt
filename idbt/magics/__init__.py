from idbt.magics.dbt import DbtMagics


def load_ipython_extension(ipython):
    ipython.register_magics(DbtMagics)
