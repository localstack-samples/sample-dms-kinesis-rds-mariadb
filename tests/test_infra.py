import json
import os
import time
from pprint import pprint
from time import sleep
from typing import Callable, TypedDict, TypeVar

import pymysql.cursors
import pytest
from boto3 import client

STACK_NAME = os.getenv("STACK_NAME", "")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")

cfn = client("cloudformation", endpoint_url=ENDPOINT_URL)
dms = client("dms", endpoint_url=ENDPOINT_URL)
kinesis = client("kinesis", endpoint_url=ENDPOINT_URL)
secretsmanager = client("secretsmanager", endpoint_url=ENDPOINT_URL)

retries = 100 if not ENDPOINT_URL else 10
retry_sleep = 5 if not ENDPOINT_URL else 1

# SQL Queries from query.py
SQL_CREATE_ACCOUNTS_TABLE = """CREATE TABLE accounts (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    age TINYINT UNSIGNED,
                    birth_date DATE,
                    account_balance DECIMAL(10, 2),
                    is_active BOOLEAN,
                    signup_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login DATETIME,
                    bio TEXT,
                    profile_picture BLOB,
                    favorite_color ENUM('red', 'green', 'blue'),
                    height FLOAT,
                    weight DOUBLE
                );"""

SQL_INSERT_ACCOUNTS_SAMPLE_DATA = """INSERT INTO accounts
(name, age, birth_date, account_balance, is_active, signup_time, last_login, bio, profile_picture, favorite_color, height, weight)
VALUES
('Alice', 30, '1991-05-21', 1500.00, TRUE, '2021-01-08 09:00:00', '2021-03-10 08:00:00', 'Bio of Alice', NULL, 'red', 1.70, 60.5);"""

SQL_CREATE_AUTHORS_TABLE = """CREATE TABLE authors (
    author_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    nationality VARCHAR(50),
    biography TEXT,
    email VARCHAR(255),
    phone_number VARCHAR(20),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""

SQL_INSERT_AUTHORS_SAMPLE_DATA = """INSERT INTO authors (first_name, last_name, date_of_birth, nationality, biography, email, phone_number)
VALUES
('John', 'Doe', '1980-01-01', 'American', 'Biography of John Doe.', 'john.doe@example.com', '123-456-7890');"""

SQL_CREATE_NOVELS_TABLE = """CREATE TABLE novels (
    novel_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    author_id INT,
    publish_date DATE,
    isbn VARCHAR(20),
    genre VARCHAR(100),
    page_count INT,
    publisher VARCHAR(100),
    language VARCHAR(50),
    available_copies INT,
    total_copies INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);
"""

SQL_INSERT_NOVELS_SAMPLE_DATA = """INSERT INTO novels (title, author_id, publish_date, isbn, genre, page_count, publisher, language, available_copies, total_copies)
VALUES
('The Great Adventure', 1, '2020-06-01', '978-3-16-148410-0', 'Adventure', 300, 'Adventure Press', 'English', 10, 20),
('Journey to the Stars', 1, '2021-04-10', '978-0-11-322456-7', 'Science Fiction', 350, 'SciFi Universe', 'English', 12, 25);"""

ALTER_TABLES = [
    # control: column-type-change -> authors
    "ALTER TABLE authors MODIFY COLUMN email VARCHAR(100)",
    # control: drop-column -> accounts
    "ALTER TABLE accounts DROP COLUMN profile_picture;",
    # control: add-column with default value -> novels
    "ALTER TABLE novels ADD COLUMN is_stock BOOLEAN DEFAULT TRUE;",
]

CREATE_TABLES = [
    SQL_CREATE_AUTHORS_TABLE,
    SQL_CREATE_ACCOUNTS_TABLE,
    SQL_CREATE_NOVELS_TABLE,
]

DROP_TABLES = [
    "DROP TABLE IF EXISTS novels;",
    "DROP TABLE IF EXISTS accounts;",
    "DROP TABLE IF EXISTS authors;",
]

PRESEED_DATA = [
    SQL_INSERT_AUTHORS_SAMPLE_DATA,
    SQL_INSERT_ACCOUNTS_SAMPLE_DATA,
    SQL_INSERT_NOVELS_SAMPLE_DATA,
]


class CfnOutput(TypedDict):
    cdcTaskSecret: str
    cdcTask1: str
    cdcTask2: str

    fullTaskSecret: str
    fullTask1: str
    fullTask2: str

    kinesisStream: str


class Credentials(TypedDict):
    host: str
    port: int
    username: str
    password: str
    dbname: str


def get_cfn_output():
    stacks = cfn.describe_stacks()["Stacks"]
    stack = None
    for s in stacks:
        if s["StackName"] == STACK_NAME:
            stack = s
            break
    if not stack:
        raise Exception(f"Stack {STACK_NAME} Not found")

    outputs = stack["Outputs"]
    cfn_output = CfnOutput()
    for output in outputs:
        cfn_output[output["OutputKey"]] = output["OutputValue"]
    return cfn_output


def get_credentials(secret_arn: str) -> Credentials:
    secret_value = secretsmanager.get_secret_value(SecretId=secret_arn)
    credentials = Credentials(**json.loads(secret_value["SecretString"]))
    if credentials["host"] == "mariadb_server":
        credentials["host"] = "localhost"
    return credentials


T = TypeVar("T")


def retry(
    function: Callable[..., T], retries=retries, sleep=retry_sleep, **kwargs
) -> T:
    raise_error = None
    retries = int(retries)
    for i in range(0, retries + 1):
        try:
            return function(**kwargs)
        except Exception as error:
            raise_error = error
            time.sleep(sleep)
    raise raise_error


def run_queries_on_mysql(
    credentials: Credentials,
    queries: list[str],
):
    cursor = None
    cnx = None
    try:
        cnx = pymysql.connect(
            user=credentials["username"],
            password=credentials["password"],
            host=credentials["host"],
            database=credentials["dbname"],
            cursorclass=pymysql.cursors.DictCursor,
            port=int(credentials["port"]),
        )
        cursor = cnx.cursor()
        for query in queries:
            cursor.execute(query)
        cnx.commit()
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def get_query_result(
    credentials: Credentials,
    query: str,
):
    cursor = None
    cnx = None
    try:
        cnx = pymysql.connect(
            user=credentials["username"],
            password=credentials["password"],
            host=credentials["host"],
            database=credentials["dbname"],
            cursorclass=pymysql.cursors.DictCursor,
            port=int(credentials["port"]),
        )
        cursor = cnx.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def start_task(task: str):
    response = dms.start_replication_task(
        ReplicationTaskArn=task, StartReplicationTaskType="start-replication"
    )
    status = response["ReplicationTask"].get("Status")
    print(f"Replication Task {task} status: {status}")


def stop_task(task: str):
    response = dms.stop_replication_task(ReplicationTaskArn=task)
    status = response["ReplicationTask"].get("Status")
    print(f"\n Replication Task {task} status: {status}")


def wait_for_task_status(task: str, expected_status: str):
    print(f"Waiting for task status {expected_status}")

    def _wait_for_status():
        status = dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-arn", "Values": [task]}],
            WithoutSettings=True,
        )["ReplicationTasks"][0].get("Status")
        print(f"{task=} {status=}")
        assert status == expected_status

    retry(_wait_for_status)


def get_table_counts(credentials: Credentials) -> dict:
    """Get row counts for all tables"""
    tables = ["authors", "accounts", "novels"]
    counts = {}
    for table in tables:
        try:
            result = get_query_result(
                credentials, f"SELECT COUNT(*) as count FROM {table}"
            )
            counts[table] = result[0]["count"] if result else 0
        except Exception:
            counts[table] = 0
    print("\n=== Table Row Counts ===")
    pprint(counts)
    return counts


def get_table_schemas(credentials: Credentials) -> dict:
    """Get schema information for all tables"""
    tables = ["authors", "accounts", "novels"]
    schemas = {}
    for table in tables:
        try:
            result = get_query_result(credentials, f"DESCRIBE {table}")
            schemas[table] = result
        except Exception:
            schemas[table] = None
    print("\n=== Table Schemas ===")
    pprint(schemas)
    return schemas


def get_all_table_data(credentials: Credentials) -> dict:
    """Get all data from all tables"""
    tables = ["authors", "accounts", "novels"]
    data = {}
    for table in tables:
        try:
            result = get_query_result(credentials, f"SELECT * FROM {table}")
            data[table] = result
        except Exception:
            data[table] = []
    print("\n=== Table Contents ===")
    pprint(data)
    return data


def execute_full_load(cfn_output: CfnOutput):
    credentials = get_credentials(cfn_output["fullTaskSecret"])
    print("\n=== Starting Full Load Test ===")
    print(f"Credentials: {credentials}")

    # Full load Flow
    threshold_timestamp = int(time.time())
    task_1 = cfn_output["fullTask1"]
    task_2 = cfn_output["fullTask2"]
    stream = cfn_output["kinesisStream"]

    print(f"\nTask ARNs:")
    print(f"Task 1: {task_1}")
    print(f"Task 2: {task_2}")
    print(f"Kinesis Stream: {stream}")

    print("*" * 12)
    print("STARTING FULL LOAD FLOW")
    print("*" * 12)
    print(f"db endpoint: {credentials['host']}:{credentials['port']}\n")

    print("\n=== Initial State ===")
    initial_counts = get_table_counts(credentials)
    initial_schemas = get_table_schemas(credentials)

    print("\tCleaning tables")
    run_queries_on_mysql(credentials, DROP_TABLES)
    print("\tCreating tables")
    run_queries_on_mysql(credentials, CREATE_TABLES)
    print("\tInserting data")
    run_queries_on_mysql(credentials, PRESEED_DATA)

    print("\n=== After Data Load ===")
    post_load_counts = get_table_counts(credentials)
    post_load_schemas = get_table_schemas(credentials)
    post_load_data = get_all_table_data(credentials)

    print("\n****Full Task 1****\n")
    print("\n\tStarting Full load task 1 a%")
    start_task(task_1)
    wait_for_task_status(task_1, "stopped")

    print("\n=== Task 1 Statistics ===")
    task1_stats = describe_table_statistics(task_1)
    pprint(task1_stats)

    # 2 drops, 2 create, 1 authors, 1 accounts = 6
    kinesis_records = wait_for_kinesis(stream, 6, threshold_timestamp)
    print("\n=== Task 1 Kinesis Records ===")
    pprint(kinesis_records)
    print("\n****End of Full Task 1****\n")

    sleep(1)
    print("\n****Full Task 2****\n")
    threshold_timestamp = int(time.time())
    print("\tStarting Full load task 2 novels")
    start_task(task_2)
    wait_for_task_status(task_2, "stopped")

    print("\n=== Task 2 Statistics ===")
    task2_stats = describe_table_statistics(task_2)
    pprint(task2_stats)

    # 1 drop, 1 create, 2 novels = 4
    kinesis_records = wait_for_kinesis(stream, 4, threshold_timestamp)
    print("\n=== Task 2 Kinesis Records ===")
    pprint(kinesis_records)
    print("\n****End of Full Task 2****\n")

    print("\n=== Final State ===")
    final_counts = get_table_counts(credentials)
    final_schemas = get_table_schemas(credentials)
    final_data = get_all_table_data(credentials)

    print("\tCleaning tables")
    run_queries_on_mysql(credentials, DROP_TABLES)

    return {
        "initial_state": {"counts": initial_counts, "schemas": initial_schemas},
        "post_load_state": {
            "counts": post_load_counts,
            "schemas": post_load_schemas,
            "data": post_load_data,
        },
        "final_state": {
            "counts": final_counts,
            "schemas": final_schemas,
            "data": final_data,
        },
        "task1_stats": task1_stats,
        "task2_stats": task2_stats,
    }


def execute_cdc(cfn_output: CfnOutput):
    credentials = get_credentials(cfn_output["cdcTaskSecret"])
    print("\n=== Starting CDC Test ===")
    print(f"Credentials: {credentials}")

    task_1 = cfn_output["cdcTask1"]
    task_2 = cfn_output["cdcTask2"]
    stream = cfn_output["kinesisStream"]

    print(f"\nTask ARNs:")
    print(f"Task 1: {task_1}")
    print(f"Task 2: {task_2}")
    print(f"Kinesis Stream: {stream}")

    print("*" * 12)
    print("STARTING CDC FLOW")
    print("*" * 12)
    print(f"db endpoint: {credentials['host']}:{credentials['port']}\n")

    print("\n=== Initial State ===")
    initial_counts = get_table_counts(credentials)
    initial_schemas = get_table_schemas(credentials)

    run_queries_on_mysql(credentials, DROP_TABLES)
    print("\tCreating tables")
    run_queries_on_mysql(credentials, CREATE_TABLES)

    print("\n=== After Table Creation ===")
    post_create_counts = get_table_counts(credentials)
    post_create_schemas = get_table_schemas(credentials)

    threshold_timestamp = int(time.time())
    print("Starting cdc tasks 1 table a%")
    start_task(task_1)
    print("Starting cdc tasks 2 table novels")
    start_task(task_2)
    wait_for_task_status(task_1, "running")
    wait_for_task_status(task_2, "running")

    print("\n****Create table events****\n")
    # 2 create apply_dms_exception, 3 create
    kinesis_records = wait_for_kinesis(stream, 5, threshold_timestamp)
    print("\n=== Create Table Events ===")
    pprint(kinesis_records)
    print("\n****End create table events****\n")

    print("\n****INSERT events****\n")
    sleep(1)
    threshold_timestamp = int(time.time())
    sleep(1)
    run_queries_on_mysql(credentials, PRESEED_DATA)

    print("\n=== After Data Insert ===")
    post_insert_counts = get_table_counts(credentials)
    post_insert_data = get_all_table_data(credentials)

    # 1 authors, 1 accounts, 2 novels
    kinesis_records = wait_for_kinesis(stream, 4, threshold_timestamp)
    print("\n=== Insert Events ===")
    pprint(kinesis_records)
    print("\n****End of INSERT events****\n")

    print("\n****ALTER tables events****\n")
    sleep(1)
    threshold_timestamp = int(time.time())
    sleep(1)
    run_queries_on_mysql(credentials, ALTER_TABLES)

    print("\n=== After Schema Changes ===")
    post_alter_schemas = get_table_schemas(credentials)

    kinesis_records = wait_for_kinesis(stream, 3, threshold_timestamp)
    print("\n=== Alter Table Events ===")
    pprint(kinesis_records)
    print("\n****End of ALTER tables events****\n")

    print("\n=== Task Statistics ===")
    print("\tTable Statistics tasks 1")
    task1_stats = describe_table_statistics(task_1)
    pprint(task1_stats)
    print("\n\tTable Statistics tasks 2")
    task2_stats = describe_table_statistics(task_2)
    pprint(task2_stats)

    stop_task(task_1)
    stop_task(task_2)
    wait_for_task_status(task_1, "stopped")
    wait_for_task_status(task_2, "stopped")

    print("\n=== Final State ===")
    final_counts = get_table_counts(credentials)
    final_schemas = get_table_schemas(credentials)
    final_data = get_all_table_data(credentials)

    print("\n\tDrop tables")
    run_queries_on_mysql(credentials, DROP_TABLES)

    return {
        "initial_state": {"counts": initial_counts, "schemas": initial_schemas},
        "post_create_state": {
            "counts": post_create_counts,
            "schemas": post_create_schemas,
        },
        "post_insert_state": {"counts": post_insert_counts, "data": post_insert_data},
        "post_alter_state": {"schemas": post_alter_schemas},
        "final_state": {
            "counts": final_counts,
            "schemas": final_schemas,
            "data": final_data,
        },
        "task1_stats": task1_stats,
        "task2_stats": task2_stats,
    }


def wait_for_kinesis(stream: str, expected_count: int, threshold_timestamp: int):
    print("\n\tKinesis events\n")
    print("fetching Kinesis event")

    shard_id = kinesis.describe_stream(StreamARN=stream)["StreamDescription"]["Shards"][
        0
    ]["ShardId"]
    shard_iterator = kinesis.get_shard_iterator(
        StreamARN=stream,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )
    shard_iter = shard_iterator["ShardIterator"]
    all_records = []
    while shard_iter is not None:
        res = kinesis.get_records(ShardIterator=shard_iter, Limit=50)
        shard_iter = res["NextShardIterator"]
        records = res["Records"]
        for r in records:
            if r["ApproximateArrivalTimestamp"].timestamp() > threshold_timestamp:
                all_records.append(r)
        if len(all_records) >= expected_count:
            break
        print(f"found {len(all_records)}, {expected_count=}")
        sleep(retry_sleep)
    print(f"Received: {len(all_records)} events")
    records_data = [
        {**json.loads(record["Data"]), "partition_key": record["PartitionKey"]}
        for record in all_records
    ]
    pprint(records_data)
    return records_data


def describe_table_statistics(task_arn: str):
    res = dms.describe_table_statistics(
        ReplicationTaskArn=task_arn,
    )
    res["TableStatistics"] = sorted(
        res["TableStatistics"], key=lambda x: (x["SchemaName"], x["TableName"])
    )
    return res


@pytest.fixture(scope="module")
def cfn_output():
    return get_cfn_output()


def test_full_load(cfn_output):
    credentials = get_credentials(cfn_output["fullTaskSecret"])
    threshold_timestamp = int(time.time())
    task_1 = cfn_output["fullTask1"]
    task_2 = cfn_output["fullTask2"]
    stream = cfn_output["kinesisStream"]

    # Clean and setup tables
    run_queries_on_mysql(credentials, DROP_TABLES)
    run_queries_on_mysql(credentials, CREATE_TABLES)
    run_queries_on_mysql(credentials, PRESEED_DATA)

    # Verify initial data load
    table_counts = get_table_counts(credentials)
    assert table_counts["authors"] == 1, "Expected 1 author record"
    assert table_counts["accounts"] == 1, "Expected 1 account record"
    assert table_counts["novels"] == 2, "Expected 2 novel records"

    # Execute and verify Task 1
    start_task(task_1)
    wait_for_task_status(task_1, "stopped")
    task1_records = wait_for_kinesis(stream, 6, threshold_timestamp)
    assert len(task1_records) == 6, "Expected 6 Kinesis records for Task 1"
    sleep(5)

    # Verify Task 1 statistics
    task1_stats = describe_table_statistics(task_1)
    authors_stats = next(
        stat
        for stat in task1_stats["TableStatistics"]
        if stat["TableName"] == "authors"
    )
    accounts_stats = next(
        stat
        for stat in task1_stats["TableStatistics"]
        if stat["TableName"] == "accounts"
    )

    # Check full load rows
    assert (
        authors_stats["FullLoadRows"] == 1
    ), "Expected 1 full load row in authors table"
    assert (
        accounts_stats["FullLoadRows"] == 1
    ), "Expected 1 full load row in accounts table"

    # Check table state
    assert (
        authors_stats["TableState"] == "Table completed"
    ), "Authors table should be completed"
    assert (
        accounts_stats["TableState"] == "Table completed"
    ), "Accounts table should be completed"

    # Check error counts
    assert (
        authors_stats["FullLoadErrorRows"] == 0
    ), "Should have no errors in authors table load"
    assert (
        accounts_stats["FullLoadErrorRows"] == 0
    ), "Should have no errors in accounts table load"

    # Execute and verify Task 2
    sleep(5)
    threshold_timestamp = int(time.time())
    start_task(task_2)
    wait_for_task_status(task_2, "stopped")
    task2_records = wait_for_kinesis(stream, 4, threshold_timestamp)
    assert len(task2_records) == 4, "Expected 4 Kinesis records for Task 2"

    # Verify Task 2 statistics
    task2_stats = describe_table_statistics(task_2)
    novels_stats = next(
        stat for stat in task2_stats["TableStatistics"] if stat["TableName"] == "novels"
    )

    # Check full load rows and state for novels
    assert (
        novels_stats["FullLoadRows"] == 2
    ), "Expected 2 full load rows in novels table"
    assert (
        novels_stats["TableState"] == "Table completed"
    ), "Novels table should be completed"
    assert (
        novels_stats["FullLoadErrorRows"] == 0
    ), "Should have no errors in novels table load"

    # Cleanup
    run_queries_on_mysql(credentials, DROP_TABLES)


def test_cdc(cfn_output):
    credentials = get_credentials(cfn_output["cdcTaskSecret"])
    task_1 = cfn_output["cdcTask1"]
    task_2 = cfn_output["cdcTask2"]
    stream = cfn_output["kinesisStream"]

    # Setup tables
    run_queries_on_mysql(credentials, DROP_TABLES)
    run_queries_on_mysql(credentials, CREATE_TABLES)

    # Start CDC tasks
    threshold_timestamp = int(time.time())
    start_task(task_1)
    start_task(task_2)
    wait_for_task_status(task_1, "running")
    wait_for_task_status(task_2, "running")

    # Verify table creation events
    create_events = wait_for_kinesis(stream, 5, threshold_timestamp)
    assert len(create_events) == 5, "Expected 5 table creation events"

    # Test INSERT operations
    sleep(1)
    threshold_timestamp = int(time.time())
    sleep(1)
    run_queries_on_mysql(credentials, PRESEED_DATA)
    insert_events = wait_for_kinesis(stream, 4, threshold_timestamp)
    assert len(insert_events) == 4, "Expected 4 insert events"

    # Verify data after inserts
    table_counts = get_table_counts(credentials)
    assert table_counts["authors"] == 1, "Expected 1 author after CDC inserts"
    assert table_counts["accounts"] == 1, "Expected 1 account after CDC inserts"
    assert table_counts["novels"] == 2, "Expected 2 novels after CDC inserts"

    # Test ALTER operations
    sleep(1)
    threshold_timestamp = int(time.time())
    sleep(1)
    run_queries_on_mysql(credentials, ALTER_TABLES)
    alter_events = wait_for_kinesis(stream, 3, threshold_timestamp)
    assert len(alter_events) == 3, "Expected 3 alter events"

    # Verify schema changes
    schemas = get_table_schemas(credentials)
    authors_email_field = next(
        field for field in schemas["authors"] if field["Field"] == "email"
    )
    assert (
        authors_email_field["Type"] == "varchar(100)"
    ), "Expected email field type to be varchar(100)"

    # Verify accounts table modification
    accounts_fields = [field["Field"] for field in schemas["accounts"]]
    assert "profile_picture" not in accounts_fields, "profile_picture should be dropped"

    # Verify novels table modification
    novels_fields = [field["Field"] for field in schemas["novels"]]
    assert "is_stock" in novels_fields, "is_stock field should be added"

    # Stop tasks and cleanup
    stop_task(task_1)
    stop_task(task_2)
    wait_for_task_status(task_1, "stopped")
    wait_for_task_status(task_2, "stopped")
    run_queries_on_mysql(credentials, DROP_TABLES)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
