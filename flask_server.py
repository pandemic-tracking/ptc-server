"""This file is the main module which contains the app.
"""
from app import create_app
from decouple import config
from flask.cli import AppGroup
import click

import config as configs

# Figure out which config we want based on the `ENV` env variable, default to local

env_config = config("ENV", cast=str, default="local")
config_dict = {
    'local': configs.LocalConfig,
}

app = create_app(config_dict[env_config]())

@app.cli.command()
def deploy():
    """Run deployment tasks"""
    # e.g. this _used_ to be where a database migration would run via `upgrade()`
    pass
