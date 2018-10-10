import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

def get_env_var(environ_str):
    return os.environ[environ_str]