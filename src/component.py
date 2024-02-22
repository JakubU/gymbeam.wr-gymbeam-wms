import csv
import logging
import json
import requests
from datetime import datetime
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# Disable requests INFO level logging
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

# Configuration variables
KEY_API_TOKEN = '#api_token'
KEY_API_URL = 'api_url'


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def create_output_table(self):
        """
        Creates the output table definition.
        """
        # Create the output table definition
        self.output_table = self.create_out_table_definition('result.csv', incremental=True)
        # Open output file, set headers, writer, and write headers
        self._output_file = open(self.output_table.full_path, 'wt', encoding='UTF-8', newline='')
        output_fields = ['endpoint', 'data', 'status_code', 'message']
        self._output_writer = csv.DictWriter(self._output_file, fieldnames=output_fields)
        self._output_writer.writeheader()

    def write_output_record(self, endpoint, data, status_code, message):
        """
        Writes a record to the output table.
        """
        self._output_writer.writerow({
            'endpoint': endpoint,
            'data': data,
            'status_code': status_code,
            'message': message
        })

    def run(self):
        input_tables = self.get_input_tables_definitions()
        for table in input_tables:
            logging.info(f'Received input table: {table.name} with path: {table.full_path}')

        if len(input_tables) == 0:
            raise UserException("No input tables found")

        # Fetch configuration values
        api_token = self.configuration.parameters.get(KEY_API_TOKEN, '')
        api_url = self.configuration.parameters.get(KEY_API_URL, '')

        # Initialize counters
        successful_requests = 0
        failed_requests = 0

        # Log the start of data writing
        logging.info("Data writing started...")

        # Create output table
        self.create_output_table()

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
                    successful_requests += 1

                    # Write output record after each request
                    self.write_output_record(
                        endpoint=endpoint,
                        data=json.dumps(modified_payload),
                        status_code=response.status_code,
                        message=response.text
                    )

                except requests.RequestException as e:
                    failed_requests += 1

        # Log the end of data writing
        logging.info(f"Data writing completed. Successful requests: {successful_requests}, Failed requests: {failed_requests}")

        # Close the output file
        self._output_file.close()


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
