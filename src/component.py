import csv
import logging
import json
import requests
from datetime import datetime

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# Configuration variables
KEY_API_TOKEN = '#api_token'
KEY_API_URL = 'api_url'


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        input_tables = self.get_input_tables_definitions()
        for table in input_tables:
            logging.info(f'Received input table: {table.name} with path: {table.full_path}')

        if len(input_tables) == 0:
            raise UserException("No input tables found")

        # Fetch configuration values
        api_token = self.configuration.parameters.get(KEY_API_TOKEN, '')
        api_url = self.configuration.parameters.get(KEY_API_URL, '')

        # Iterate over input data
        input_table = input_tables[0]
        with open(input_table.full_path, 'r') as inp_file:
            reader = csv.DictReader(inp_file)

            for in_row in reader:
                # Extract relevant data from the 'data' column
                data_column = in_row.get('data', '{}')
                try:
                    data = json.loads(data_column)
                except json.JSONDecodeError as e:
                    logging.warning(f"Error decoding JSON in 'data' column: {e}")
                    continue

                # Upraviť štruktúru payloadu
                modified_payload = {
                    "data": [data.get('data', {})]
                }

                # Construct the URL using the provided endpoint from the input table
                endpoint = in_row.get('endpoint', '')
                url = f"{api_url}/{endpoint}"

                # Set up headers
                headers = {
                    'Content-Type': 'application/json',
                    'x-api-key': api_token
                }

                # Make the POST request
                try:
                    response = requests.post(url, json=modified_payload, headers=headers)
                    response.raise_for_status()
                    logging.info(f"POST request to {url} successful. Response: {response.text}")
                except requests.RequestException as e:
                    logging.error(f"Error making POST request to {url}: {e}")

        # Continue with the rest of your code...

if __name__ == "__main__":
    try:
        comp = Component()
        # This triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
