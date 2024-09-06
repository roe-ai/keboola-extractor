"""
Template Component main class.

"""

import csv
import logging
import os

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
        csv_file_name = os.path.join(self.tables_out_path, "result.csv")
        output_table_definition = self.create_out_table_definition("result.csv")

        try:
            # Write the data to the CSV file with proper quoting
            with open(
                output_table_definition.full_path,
                mode="w",
                newline="",
                encoding="utf-8",
            ) as file:
                writer = csv.writer(file, quoting=csv.QUOTE_ALL)

                # Write the header
                writer.writerow(column_names)

                # Write the data rows
                writer.writerows(result_rows)

            self.write_manifest(output_table_definition)
            logging.info("Data successfully written to %s", csv_file_name)
        except Exception as e:
            logging.error("Failed to write data to CSV: %s", e)
            raise UserException(f"Failed to write data to CSV: {e}")


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
