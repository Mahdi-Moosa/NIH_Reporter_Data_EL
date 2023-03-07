import configparser
from email.mime import base
import json
import requests
import os

config = configparser.ConfigParser()
config.read('config.cfg')

# Base URL provides baseline biorxiv url
base_url = config.get('URL', 'base_url')

doi_of_interest = '10.1101/2021.04.23.440992'

def fetch_media_impact(doi_of_interest: str, base_url: str) -> json:
    '''Constructs biorxiv URL from base_url and doi.
    Gets social media impact data as reported in biorxiv.org
    Saves json file.
    Input:
        doi_of_interest: DOI that needs to be fetched.
        base_url: Biorxiv base URL.
    Returns:
        Json file.
    '''
    url = base_url + doi_of_interest
    response = requests.get(url)
    decoded_data = response.text.encode().decode('utf-8-sig')
    json_data = json.loads(decoded_data.lstrip('\(').rstrip(')'))
    return json_data

data = fetch_media_impact(doi_of_interest=doi_of_interest, base_url=base_url)

# Define your path and file name
path = "social_media_data"
file_name = "publication_social"

# Creating save directory, if not exists.
if not os.path.isdir(path):
    os.makedirs(path)

# Write data to JSON file
with open(f"./{path}/{file_name}.json", "w") as write_file:
    json.dump(data, write_file)