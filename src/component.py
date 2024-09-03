"""
Template Component main class.

"""

import csv
import logging

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
import requests

from configuration import Configuration


class Component(ComponentBase):
    """
    Extends base class for general Python components. Initializes the CommonInterface
    and performs configuration validation.

    For easier debugging the data folder is picked up by default from `../data` path,
    relative to working directory.

    If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def run(self):
        """
        Main execution code
        """

        # check for missing configuration parameters
        params = Configuration(**self.configuration.parameters)

        api_token = params.api_token
        table_name = params.table_name
        query = params.query
        if not query:
            query = f"SELECT * FROM {table_name}"

        query_url = "https://api.roe-ai.com/v1/database/query/"
        headers = {"Authorization": f"Bearer {api_token}"}
        payload = {
            "query": query,
        }
        response = requests.request("POST", query_url, json=payload, headers=headers)
        if response.status_code >= 200 and response.status_code < 300:
            logging.info("Successfully loaded data for query: %s", query)
        else:
            logging.info(
                "Failed to load data for query: %s Error: %s", query, response.text
            )

        # Extract column names and result rows from the API response
        result = response.json()[0]
        logging.info(f"Result: {result}")
        column_names = result["column_names"]
        result_rows = result["result_rows"]

        # Define the CSV file name
        csv_file_name = os.path.join(ci.tables_out_path, "result.csv")

        try:
            # Write the data to the CSV file with proper quoting
            with open(csv_file_name, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file, quoting=csv.QUOTE_ALL)

                # Write the header
                writer.writerow(column_names)

                # Write the data rows
                writer.writerows(result_rows)

            logging.info("Data successfully written to %s", csv_file_name)
            print(f"Data successfully written to {csv_file_name}")
        except Exception as e:
            logging.error("Failed to write data to CSV: %s", e)
            print(f"Failed to write data to CSV: {e}")

        # # Access parameters in configuration
        # if params.print_hello:
        #     logging.info("Hello World")

        # # get input table definitions
        # input_tables = self.get_input_tables_definitions()
        # for table in input_tables:
        #     logging.info(f'Received input table: {table.name} with path: {table.full_path}')

        # if len(input_tables) == 0:
        #     raise UserException("No input tables found")

        # # get last state data/in/state.json from previous run
        # previous_state = self.get_state_file()
        # logging.info(previous_state.get('some_parameter'))

        # # Create output table (Table definition - just metadata)
        # table = self.create_out_table_definition('output.csv', incremental=True, primary_key=['timestamp'])

        # # get file path of the table (data/out/tables/Features.csv)
        # out_table_path = table.full_path
        # logging.info(out_table_path)

        # # Add timestamp column and save into out_table_path
        # input_table = input_tables[0]
        # with (open(input_table.full_path, 'r') as inp_file,
        #       open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file):
        #     reader = csv.DictReader(inp_file)

        #     columns = list(reader.fieldnames)
        #     # append timestamp
        #     columns.append('timestamp')

        #     # write result with column added
        #     writer = csv.DictWriter(out_file, fieldnames=columns)
        #     writer.writeheader()
        #     for in_row in reader:
        #         in_row['timestamp'] = datetime.now().isoformat()
        #         writer.writerow(in_row)

        # # Save table manifest (output.csv.manifest) from the Table definition
        # self.write_manifest(table)

        # # Write new state - will be available next run
        # self.write_state_file({"some_state_parameter": "value"})

        # # ####### EXAMPLE TO REMOVE END


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
