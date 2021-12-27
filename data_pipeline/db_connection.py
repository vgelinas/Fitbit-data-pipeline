"""
Wrapper for the sqlalchemy.create_engine method, initialising with config file.
"""
import json
import sqlalchemy


def create_engine():
    """Wrapper for the sqlalchemy.create_engine function. Load config from json
    file and instantiate a sqlalchemy engine with it.
    """
    config_filepath = ("/absolute/path/to/project/folder/" 
                       "/configs/db_config.json")

    with open(config_filepath) as f:
        db_config = json.load(f)

    arg = "{db_type}+{con}://{usr}:{pw}@{host}/{db}".format(**db_config)
    return sqlalchemy.create_engine(arg)
