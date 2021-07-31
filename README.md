## Overview

This algorithm was created to demonstrate how various features of the Algorithmia platform can be combined together to handle complex orchestration tasks. It is designed to be flexible and is intended to provide inspiration—users are encouraged to borrow these concepts as they see fit and to substitute components such as storage, database, etc., where required, to meet the specific needs of their organization and use cases.

## Steps

At a high level, this algorithm contains the following steps:
* Read database credentials from an Algorithmia hosted data collection.
* Create an Algorithmia hosted data collection to store output data.
* Establish a database connection (to Snowflake, in this case).
* Process both streaming and batch input.
* Return output and write asynchronously to the database and an Algorithmia hosted data collection.

## Setup

This algorithm relies on an Algorithmia hosted data collection as well as access to a
Snowflake instance. The following supporting files are available in the algorithm source code:
* `orch_v1_setup.sql`
    * In your Snowflake session, run the SQL commands in this file to set up the example Snowflake resources with which this algorithm interacts.
    * Note that the schema used in this demo is provided solely as an example. The attributes are made up and do not have a meaningful real-world application in any specific model.


* `config_orch_v1.json`
    * Set up a [hosted data collection](https://algorithmia.com/developers/data/hosted) under your user account. In the source code, the demo data collection is named `config`. If you name your collection something else, you'll need to modify the value of the `INPUT_COLLECTION`  variable in `SnowflakeAsyncOrchestrator.py`.
    * Copy the contents of this file into a file with the same name in the hosted data collection and fill in your private account-specific Snowflake credentials.


* `SnowflakeAsyncOrchestrator.py`
    * If you're running this algorithm from within the Web IDE, you don't need to supply an API key to instantiate the client.
    * If you're running this algorithm from outside of Algorithmia, the `ALGORITHMIA_API_KEY` environment variable is used, so it must be set in your shell. It's recommended that you use an environment variable so that you don't accidentally expose your secret credentials.

## Usage

### Input

Input type is determined by the algorithm automatically based on data type:

* _Streaming_: an **array** (see JSON structure below) containing inputs for one or more records
* _Batch_: a **string** denoting the batch ID of a series of database records

### Output

* _Streaming_: an array of model results and the location to which they were saved
* _Batch_: the output table name and number of rows inserted

### Side Effects

* _Streaming_: results are written to a Hosted Data Collection and (asynchronously) to the database
* _Batch_: results are written to the database

## Examples

### Example 1.

INPUT
```
{
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
````

OUTPUT
```
{
    "results": [
        {
            "lead_id": "0001EXAMPL",
            "score_tstamp": "2021-01-20 18:39:29",
            "score_value": 42
        },
        {
            "lead_id": "0002EXAMPL",
            "score_tstamp": "2021-01-20 18:39:30",
            "score_value": 42
        }
    ],
    "output_data_path": "data://<username>/orch_v1/101.json"
}
````

### Example 2.

INPUT
```
"2"
````

OUTPUT
```
{
    "output_snowflake_table": "test_output_table",
    "num_rows_inserted": 3
}
```