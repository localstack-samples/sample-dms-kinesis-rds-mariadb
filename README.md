# Sample Application showcasing how to use DMS to create CDC

## Introduction

This scenario demonstrates how to use Database Migration Service (DMS) to create change data capture (CDC) and full load tasks using the Cloud Development Kit in Python. It is a self-contained setup that will create a VPC to host 2 databases, a Kinesis stream, and 4 replication tasks.

## Pre-requisites

-   [LocalStack Auth Token](https://docs.localstack.cloud/getting-started/auth-token/)
-   [Python 3.10](https://www.python.org/downloads/) & `pip`
-   [Docker Compose](https://docs.docker.com/compose/install/)
-   [CDK](https://docs.localstack.cloud/user-guide/integrations/aws-cdk/)  with the  [`cdklocal`](https://github.com/localstack/aws-cdk-local) wrapper.

  
Start LocalStack Pro with the `LOCALSTACK_AUTH_TOKEN`  pre-configured:

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
docker-compose up
```

The Docker Compose file will start LocalStack Pro container and a MariaDB container. The MariaDB container will be used to showcase how to reach a database external to LocalStack.

## Instructions

### Install the dependencies

Install all the dependencies by running the following command:

```bash
make install
```

### Creating the infrastructure

To deploy the infrastructure, you can run the following command:

```bash
make deploy
```

After successful deployment, you will see the following output:

```bash
Outputs:
DMsSampleSetupStack.cdcTask1 = arn:aws:dms:us-east-1:000000000000:task:A001NYMR4Z0NK45ZBJT6954RNMGEKL2PQ9XQYR4
DMsSampleSetupStack.cdcTask2 = arn:aws:dms:us-east-1:000000000000:task:GO5RC4J6CKZWSJKF4CGB6ZV3ZEMGI38DFPJF2ZU
DMsSampleSetupStack.cdcTaskSecret = arn:aws:secretsmanager:us-east-1:000000000000:secret:DMsSampleSetupStack-rdsinstanceSecret07FEB42-907ed0cf-RSPkZq
DMsSampleSetupStack.fullTask1 = arn:aws:dms:us-east-1:000000000000:task:BCZLANJP9WFXKNTYBEWTAQ1YHIVJ5C2ZUIHDPB2
DMsSampleSetupStack.fullTask2 = arn:aws:dms:us-east-1:000000000000:task:ZO7WPZTTAKOA1CONK2Y3Y0H6FXLAFWUYX1OPGPM
DMsSampleSetupStack.fullTaskSecret = arn:aws:secretsmanager:us-east-1:000000000000:secret:DMsSampleSetupStack-mariadbaccesssecret40AD7-611fcbcd-IKWDDh
DMsSampleSetupStack.kinesisStream = arn:aws:kinesis:us-east-1:000000000000:stream/DMsSampleSetupStack-TargetStream3B4B2880-02dd0371
Stack ARN:
arn:aws:cloudformation:us-east-1:000000000000:stack/DMsSampleSetupStack/b8298866

✨  Total time: 49.33s
```

### Running the tasks

You can run the tasks by executing the following command:

```bash
make run
```

## Developer Notes

Four tasks are deployed with the stack, split into two parts.

First, a full load replication task runs against the external DB:

-   Creates three tables: `authors`, `accounts`, `novels`
-   Makes four inserts
-   Starts full load task 1 targeting tables starting with 'a' (`a%` table mapping)
-   Captures and logs six Kinesis events: 2 drop tables, 2 create tables, 2 inserts
-   Starts full load task 2 targeting the `novels` table (`novels` table mapping)
-   Captures and logs four Kinesis events: 1 drop table, 1 create table, 2 inserts
-   Logs `table_statistics` for both tasks

Next, a CDC replication task runs against the RDS database:

-   Creates three tables: `authors`, `accounts`, `novels`
-   Starts CDC task 1 targeting tables starting with 'a' (`a%` table mapping)
-   Starts CDC task 2 targeting the `novels` table (`novels` table mapping)
-   Captures and logs five Kinesis events: 2 for `awsdms_apply_exceptions` table, 3 for our tables
-   Makes four inserts
-   Captures and logs four Kinesis events: 2 for tables in task 1, 2 for table in task 2
-   Makes three table alterations, one per table
-   Captures and logs three Kinesis events
-   Logs `table_statistics` for both tasks

Two tasks perform full load replication on Dockerized MariaDB. The other two perform CDC replication on a MariaDB RDS database.

All tasks target the same Kinesis Stream.

## Deploying on AWS

You can deploy and run the stack on AWS by running the following commands:

```bash
make deploy-aws
make run-aws
```

## License

This project is licensed under the Apache 2.0 License.
