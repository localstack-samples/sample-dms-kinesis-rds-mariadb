import json
import os
from pprint import pprint
from time import sleep
import time
from typing import Callable, TypeVar, TypedDict

from boto3 import client
import pymysql.cursors

from lib import query as q

STACK_NAME = os.getenv("STACK_NAME", "")

ENDPOINT_URL = os.getenv("ENDPOINT_URL")

cfn = client("cloudformation", endpoint_url=ENDPOINT_URL)
dms = client("dms", endpoint_url=ENDPOINT_URL)
kinesis = client("kinesis", endpoint_url=ENDPOINT_URL)
secretsmanager = client("secretsmanager", endpoint_url=ENDPOINT_URL)


retries = 100 if not ENDPOINT_URL else 10
retry_sleep = 5 if not ENDPOINT_URL else 1


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
    credentials =  Credentials(**json.loads(secret_value["SecretString"]))
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
    pprint(
        [
            {**json.loads(record["Data"]), "partition_key": record["PartitionKey"]}
            for record in all_records
        ]
    )


def describe_table_statistics(task_arn: str):
    res = dms.describe_table_statistics(
        ReplicationTaskArn=task_arn,
    )
    res["TableStatistics"] = sorted(
        res["TableStatistics"], key=lambda x: (x["SchemaName"], x["TableName"])
    )
    return res


def execute_full_load(cfn_output: CfnOutput):
    credentials = get_credentials(cfn_output["fullTaskSecret"])
    # Full load Flow
    threshold_timestamp = int(time.time())
    task_1 = cfn_output["fullTask1"]
    task_2 = cfn_output["fullTask2"]
    stream = cfn_output["kinesisStream"]

    print("*" * 12)
    print("STARTING FULL LOAD FLOW")
    print("*" * 12)
    print(f"db endpoint: {credentials['host']}:{credentials['port']}\n")
    print("\tCleaning tables")
    run_queries_on_mysql(credentials, q.DROP_TABLES)
    print("\tCreating tables")
    run_queries_on_mysql(credentials, q.CREATE_TABLES)
    print("\tInserting data")
    run_queries_on_mysql(credentials, q.PRESEED_DATA)

    authors = get_query_result(
        credentials, "SELECT first_name, last_name FROM authors"
    )
    accounts = get_query_result(
        credentials, "SELECT name, account_balance FROM accounts"
    )
    novels = get_query_result(credentials, "SELECT title, author_id FROM novels")
    print("\n\tAdded the following authors")
    pprint(authors)
    print("\n\tAdded the following accounts")
    pprint(accounts)
    print("\n\tAdded the following novels")
    pprint(novels)

    print("\n****Full Task 1****\n")
    print("\n\tStarting Full load task 1 a%")
    start_task(task_1)
    wait_for_task_status(task_1, "stopped")
    # 2 drops, 2 create, 1 authors, 1 accounts = 6
    wait_for_kinesis(stream, 6, threshold_timestamp)
    print("\n****End of Full Task 1****\n")

    sleep(1)
    print("\n****Full Task 2****\n")
    threshold_timestamp = int(time.time())
    print("\tStarting Full load task 2 novels")
    start_task(task_2)
    wait_for_task_status(task_2, "stopped")
    # 1 drop, 1 create, 2 novels = 4
    wait_for_kinesis(stream, 4, threshold_timestamp)
    print("\n****End of Full Task 2****\n")

    print("\n****Table Statistics****\n")
    print("\tTable Statistics tasks 1")
    pprint(describe_table_statistics(task_1))
    print("\n\tTable Statistics tasks 2")
    pprint(describe_table_statistics(task_2))

    print("\tCleaning tables")
    run_queries_on_mysql(credentials, q.DROP_TABLES)


def execute_cdc(cfn_output: CfnOutput):
    # CDC Flow
    credentials = get_credentials(cfn_output["cdcTaskSecret"])
    task_1 = cfn_output["cdcTask1"]
    task_2 = cfn_output["cdcTask2"]
    stream = cfn_output["kinesisStream"]
    print("")
    print("*" * 12)
    print("STARTING CDC FLOW")
    print("*" * 12)
    print(f"db endpoint: {credentials['host']}:{credentials['port']}\n")

    run_queries_on_mysql(credentials, q.DROP_TABLES)
    print("\tCreating tables")
    run_queries_on_mysql(credentials, q.CREATE_TABLES)

    threshold_timestamp = int(time.time())
    print("Starting cdc tasks 1 table a%")
    start_task(task_1)
    print("Starting cdc tasks 2 table novels")
    start_task(task_2)
    wait_for_task_status(task_1, "running")
    wait_for_task_status(task_2, "running")

    print("\n****Create table events****\n")
    # 2 create apply_dms_exception, 3 create
    wait_for_kinesis(stream, 5, threshold_timestamp)
    print("\n****End create table events****\n")

    print("\n****INSERT events****\n")
    sleep(1)
    threshold_timestamp = int(time.time())
    sleep(1)
    run_queries_on_mysql(credentials, q.PRESEED_DATA)
    # 1 authors, 1 accounts, 2 novels
    wait_for_kinesis(stream, 4, threshold_timestamp)
    print("\n****End of INSERT events****\n")

    print("\n****ALTER tables events****\n")
    sleep(1)
    threshold_timestamp = int(time.time())
    sleep(1)
    run_queries_on_mysql(credentials, q.ALTER_TABLES)
    wait_for_kinesis(stream, 3, threshold_timestamp)
    print("\n****End of ALTER tables events****\n")

    print("\n****Table Statistics****\n")
    print("\tTable Statistics tasks 1")
    pprint(describe_table_statistics(task_1))
    print("\n\tTable Statistics tasks 2")
    pprint(describe_table_statistics(task_2))

    stop_task(task_1)
    stop_task(task_2)
    wait_for_task_status(task_1, "stopped")
    wait_for_task_status(task_2, "stopped")

    print("\n\tDrop tables")
    run_queries_on_mysql(credentials, q.DROP_TABLES)


if __name__ == "__main__":
    cfn_output = get_cfn_output()

    execute_full_load(cfn_output)
    execute_cdc(cfn_output)
