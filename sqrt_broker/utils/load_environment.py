from dotenv import load_dotenv
import os
from sqrt_broker.utils.broker_exceptions import ENVException

HOME_DIR = os.path.expanduser("~")


def load_env(env_path):
    # The env_path file must be in f"{HOME}/.env_sqrt/"
    if not load_dotenv(f"{HOME_DIR}/.env_sqrt/{env_path}", override=True):
        raise ENVException(f"Fail to load {HOME_DIR}/.env_sqrt/{env_path}")
    else:
        return True
