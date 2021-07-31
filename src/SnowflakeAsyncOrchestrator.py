import asyncio
from functools import partial
from itertools import chain
import json
from multiprocessing.pool import ThreadPool
import os
from queue import Queue
from time import sleep
from typing import Callable, Tuple, Union

import Algorithmia
from Algorithmia.errors import AlgorithmException, DataApiError
import snowflake.connector


##########################
#### BEGIN USER EDITS ####

# Instantiate the Algorithmia client, which handles the API requests.
# For testing in the Web IDE, no API key is needed.
client = Algorithmia.client()

## If calling the algorithm from outside of the Web IDE, an API key is required.
## This may be supplied as the ALGORITHMIA_API_KEY environment variable.
#ALGORITHMIA_API_KEY = os.getenv("ALGORITHMIA_API_KEY", None)
#if ALGORITHMIA_API_KEY is None:
#    raise Exception("Please set a valid ALGORITHMIA_API_KEY environment variable.")
#client = Algorithmia.client(ALGORITHMIA_API_KEY)


# Define data and algorithm paths. Here they are stored as constants that can
# easily be configured by the algorithm developer. In this case, there is a
# data collection named `orch_v1` to hold the output, and the credentials are
# stored in `config_orch_v1.json` in the user's `config` collection under their
# username.
INPUT_COLLECTION = "config"
CONFIG_FILENAME = "config_orch_v1.json"
OUTPUT_COLLECTION = "orch_v1"
ALGORITHM = "algo://algorithmiahq/demo_leadscore_A1/1.0.1"

##### END USER EDITS ######
###########################

ALGORITHMIA_USERNAME = client.username()

CREDENTIALS_PATH = "data://{}/{}/{}".format(
    ALGORITHMIA_USERNAME, INPUT_COLLECTION, CONFIG_FILENAME)
OUTPUT_PATH = "data://{}/{}".format(
    ALGORITHMIA_USERNAME, OUTPUT_COLLECTION)


# Create output collection if it doesn't already exist.
OUTPUT_DIR = client.dir(OUTPUT_PATH)
if not OUTPUT_DIR.exists():
    OUTPUT_DIR.create()


# Set a few additional constants to specify the maximum safe number of requests
# to return in an API response, and the maximum number of times to retry an
# algorithm request if an exception has occured.
MAX_RESULTS_RETURNABLE = 20
MAX_RETRIES = 5

# Assign default values for the orchestration parameters. These can be
# overridden by the algorithm caller within the JSON input passed to apply().
BATCH_SIZE = 2
POOL_SIZE = 5

# If something fails in the asyncronous processing, enable the main thread to
# terminate and return the error message.
FAILURE_QUEUE = Queue(1)


def parse_JSON_to_dict(path: str) -> dict:
    """Read a JSON file and return a Python dictionary."""
    return client.file(path).getJson()


def get_snowflake_conn(snowflake_creds: dict) -> (
        snowflake.connector.connection.SnowflakeConnection):
    """
    Open a connection to Snowflake using the given credentials.

    The database connection only needs to be opened once. For more information
    regarding parameters accepted, see the Snowflake docs:
        https://docs.snowflake.com/en/user-guide/
        python-connector-api.html#connect
    """
    conn = snowflake.connector.connect(
        user=snowflake_creds["user"],
        password=snowflake_creds["password"],
        account=snowflake_creds["account"],
        application=snowflake_creds["application"],
        warehouse=snowflake_creds["warehouse"],
        database=snowflake_creds["database"],
        schema=snowflake_creds["schema"],
    )
    return conn


def get_fully_qualified_tablename(database: str,
                                  schema: str,
                                  table: str) -> str:
    return("{}.{}.{}".format(database, schema, table))


def fetch_batch_input_from_snowflake(batch_id: str) -> list:
    """
    Filter Snowflake table for given batch_id and return matching records.

    Notes about Snowflake syntax:
        1) Snowflake is particular about case sensitivity. Here, all db
           objects are uppercased because they are defined as uppercase
           in the SQL script. For more, see the Snowflake docs:
                https://docs.snowflake.com/en/sql-reference/
                identifiers-syntax.html

        2) The IDENTIFIER keyword is used to identify db objects specified
           using bind variables. For more, see the Snowflake docs:
                https://docs.snowflake.com/en/sql-reference/
                identifier-literal.html

        3) This query uses the default `paramstyle="pyformat"` option to bind
           parameters in the query string. For more, see the Snowflake docs:
                https://docs.snowflake.com/en/user-guide/
                python-connector-api.html#paramstyle
    """
    cur = CONN.cursor(snowflake.connector.DictCursor)
    bind_params = {
        "database": SNOWFLAKE_CREDS["database"].upper(),
        "schema": SNOWFLAKE_CREDS["schema"].upper(),
        "table": SNOWFLAKE_CREDS["input_table"].upper(),
        "id": batch_id
    }
    bind_params["fully_qualified_tablename"] = get_fully_qualified_tablename(
        bind_params["database"], bind_params["schema"], bind_params["table"])
    command = ("""SELECT * FROM IDENTIFIER(%(fully_qualified_tablename)s)
                  WHERE batch_id = %(id)s""")
    # Retrieve list of dicts (each dict represents one record) from database.
    results = cur.execute(command, bind_params).fetchall()
    cur.close()
    return results


def prepare_bulk_insert_query(batch_results: list) -> str:
    """
    Prepare a Snowflake query to bulk-insert batch results.

    This query uses the default `paramstyle="pyformat"` option to bind
    parameters in the query string. For more, see the Snowflake docs:
        https://docs.snowflake.com/en/user-guide/
        python-connector-api.html#paramstyle
    """
    multiple_values = []
    keys = []
    for result in batch_results:
        keys = result.keys()
        values = []
        joined_values = None
        for key in keys:
            if result[key] is None:
                values.append(str(0))
            else:
                if isinstance(result[key], str):
                    result[key] = "'{}'".format(result[key])
                elif isinstance(result[key], dict):
                    result[key] = json.dumps(result[key])
                values.append(str(result[key]))
        joined_values = "({})".format(",".join(values))
        multiple_values.append(joined_values)
    multiple_joined_values = ",".join(multiple_values)
    joined_keys = "({})".format(",".join(keys))

    bind_params = {
        "database": SNOWFLAKE_CREDS["database"].upper(),
        "schema": SNOWFLAKE_CREDS["schema"].upper(),
        "table": SNOWFLAKE_CREDS["output_table"].upper(),
    }
    bind_params["fully_qualified_tablename"] = get_fully_qualified_tablename(
        bind_params["database"], bind_params["schema"], bind_params["table"])
    # Use normal string formatting here to accept variable number of column
    # names and values. This data comes directly from the model output.
    command = """INSERT INTO IDENTIFIER(%(fully_qualified_tablename)s)
                {} VALUES {}""".format(joined_keys, multiple_joined_values)
    # The following will insert a dummy score; use during development if the
    # payload schema does not yet conform to that of the database output table.
    # command = """INSERT INTO IDENTIFIER(%(fully_qualified_tablename)s)
    #              (score_value) VALUES (999)"""
    return command, bind_params


def push_output_to_snowflake(batch_results: list) -> Tuple[str, int]:
    """
    Insert batch results into the configured Snowflake database table.
    """
    command, bind_params = prepare_bulk_insert_query(batch_results)
    cur = CONN.cursor(snowflake.connector.DictCursor)
    cur.execute(command, bind_params)
    cur.close()
    return SNOWFLAKE_CREDS["output_table"], len(batch_results)


def async_fire(f: Callable) -> Callable:
    """
    Decorate a given function for async execution.
    """
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(
            None, f, *args, *kwargs)
    return wrapped


@async_fire
def async_write_snowflake(path: str, payload: dict, queue: Queue) -> None:
    """
    Asynchronously upload a JSON blob to a given path in an Algorithmia
    Hosted Data Collection and write results to a Snowflake table. If an
    exception occurs, terminate the Snowflake connection and reconnect later.
    """
    try:
        client.file(path).putJson(payload)
        push_output_to_snowflake(payload)
    except Exception as e:
        print("exception was raised")
        queue.put((str(e), str(e.stracktrace)))


def create_request_batches(requests: list, batch_size: int) -> list:
    """
    Create batches of requests from the input sequence to satisfy a particular
    batch size upper limit.
    """
    output = []
    for i in range(0, len(requests), batch_size):
        output.append(requests[i: i + batch_size])
    return output


def worker_algorithm_with_backoff(request_batch: list,
                                  # TODO: verify client type
                                  client: Algorithmia.Client,
                                  max_backoff: int,
                                  backoff_counter: int = 1
                                  ) -> Union[list, Callable]:
    """
    Trigger an Algorithmia Algorithm request to `ALGORITHM` with the 'request'
    field being the input. If an individual request fails, the system has an
    increasing backoff to retry `max_backoff` number of times before failing.
    """
    model_algo = ALGORITHM
    try:
        results = []
        for req in request_batch:
            output = client.algo(model_algo).pipe(req).result
            results.append(output)
        return results
    except AlgorithmException as e:
        if backoff_counter < max_backoff:
            sleep(backoff_counter)
            print(e)
            return worker_algorithm_with_backoff(
                request_batch, client, max_backoff, backoff_counter + 1
            )
        else:
            raise e


def apply(input: Union[str, list]):
    """Ingest input and apply Algorithm.

    If input is not a string, it is expected to be a dictionary of format:
    {
        # REQUIRED keys
        "requests": [
            {
                "lead_id": "0008EXAMPL",
                "lead_zip": "98101",
                "is_licensed": True
            },
            {
                ...
            },
        ],
        # OPTIONAL keys
        "payload_id": 101,
        "metadata": {
            "batch_size": 2,
            "pool_size": 4
        }
    }
    """
    if FAILURE_QUEUE.full():
        raise Exception(FAILURE_QUEUE.get())
    is_batch = False
    # If input is a string, it is assumed to be a batch ID number, which
    # will be used to fetch batch input from the database.
    if isinstance(input, str):
        is_batch = True
        data = {"requests": fetch_batch_input_from_snowflake(input)}
        # Snowflake returns column names as uppercase, so lowercase them.
        data["requests"] = [
            {k.lower(): v for k, v in record.items()}
            for record in data["requests"]
        ]
        # Verify that database records were actually returned.
        if not data["requests"]:
            output = "No database records found for batch_id: {}".format(input)
            return output
    # Otherwise, input is assumed to be a dictionary (see format in docstring).
    else:
        data = input
    payload_id = data.get("payload_id", "000")
    if "metadata" not in data:
        data["metadata"] = {
            "batch_size": BATCH_SIZE,
            "pool_size": POOL_SIZE,
        }
    meta_data = data["metadata"]
    batch_size = meta_data["batch_size"]
    pool_size = meta_data["pool_size"]
    pool = ThreadPool(pool_size)
    requests = data["requests"]
    request_batches = create_request_batches(requests, batch_size)
    results = pool.map(
        partial(
            worker_algorithm_with_backoff,
            client=client,
            max_backoff=MAX_RETRIES,
            backoff_counter=1,
        ),
        request_batches,
    )
    # Flatten one level of lists.
    results_lists = list(chain.from_iterable(results))
    # Flatten one more level to yield list of dicts.
    results_dicts = list(chain.from_iterable(results_lists))

    # Write results to database and Hosted Data Collection.
    if is_batch:
        output_table, num_rows_inserted = push_output_to_snowflake(
            results_dicts)
        output = {
            "output_snowflake_table": output_table,
            "num_rows_inserted": num_rows_inserted,
        }
    else:
        output_data_path = "{}/{}.json".format(OUTPUT_COLLECTION, payload_id)
        async_write_snowflake(output_data_path, results_dicts, FAILURE_QUEUE)
        output = {"results": results_dicts,
                  "output_data_path": output_data_path}
    return output


# Retrieve Snowflake credentials from the Hosted Data Collection and
# establish a database connection. The credentials and connection object are
# set as global variables to contain Snowflake state. The connection remains
# open for the duration of the program.
CREDENTIALS = parse_JSON_to_dict(CREDENTIALS_PATH)
SNOWFLAKE_CREDS = CREDENTIALS["snowflake"]
CONN = get_snowflake_conn(SNOWFLAKE_CREDS)


# Define some test data for the demonstration.
input_1 = {
    "payload_id": 101,
    "requests": [
        {
            "lead_id": "0001EXAMPL",
            "lead_zip": "98101",
            "is_licensed": True
        },
        {
            "lead_id": "0002EXAMPL",
            "lead_zip": "98101",
            "is_licensed": True
        }
    ],
    # OPTIONAL
    "metadata": {
        "batch_size": 2,
        "pool_size": 4
    }
}

tests = [
    # streaming; process data in json-like structure in real time
    input_1,
    # batch; retrieve input from database
    "1",
    "2",
    "3",
]

if __name__ == "__main__":
    for input in tests:
        result = apply(input)
        print(result)
