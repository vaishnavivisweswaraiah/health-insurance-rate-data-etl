import requests
import json
import yaml

def read_api_config(yaml_file_name):
    # Read config yaml file
    with open(yaml_file_name) as f:
        config_data=yaml.safe_load(f)
    return config_data
def make_api_call(url):
    respone = requests.get(url)
    return respone