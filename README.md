# Dms Sample

This scenario demonstrate how to use DMS to create cdc and full load tasks using the CDK in Python.
It is a self contained scenario that will create a vpc to hosts 2 databases, a kinesis stream and 4 replication tasks.

The results from the `run` can also be seen in the `run.out` file.

# Requirements

- python3.10+
- [Aws cdk](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html)
- [cdklocal](https://www.npmjs.com/package/aws-cdk-local)

# Installation

The Makefile contains all you need to install start and run this sample.

The `install` command will create python virtual environment and install the required python dependencies. You can control the venv path by exporting `VENV_DIR` to your environment. Defaults to `.venv`

```
make install
```

# Deploy the stack

To start with Localstack in detached mode, simply run the `deploy` command. 
It will first start the docker containing Localstack and an mariadb container. This DB will be used to showcase how to reach a database external to Localstack.
Then it will deploy the CDK stack to Loacalstack

When deploying against AWS, both databases will be rds DBs.

```
# to run against Localstack
LOCALSTACK_AUTH_TOKEN=<Auth_Token> make deploy

# to run against aws
make deploy-aws
```

When running against Localstack, if you would rather see the Localstack logs, you can start the docker in a separate terminal before deploying the stack.

```
LOCALSTACK_AUTH_TOKEN=<Auth_Token> docker-compose up
```

# Run the tasks

There are four tasks that have been deployed with the stack. The run is broken down in two parts.

First we run a full load replication task against the external db.

- 3 tables are created: `authors`, `accounts` and `novels`
- 4 inserts are made
- Full load task 1 is started targetting tables starting with an `a`. Table mapping: `a%`
- Capture and log of 6 kinesis event. 2 drop tables, 2 create tables and 2 inserts.
- Full load task 2 is started targetting table `novels`. Table mapping: `novels`
- Capture and log of 4 kinesis event. 1 drop table, 1 create table and 2 inserts.
- Log of `table_statistics` for both tasks

Then we run a cdc replication task against the rds database.

- 3 tables are created: `authors`, `accounts` and `novels`
- Cdc task 1 is started targetting tables starting with an `a`. Table mapping: `a%`
- Cdc task 2 is started targetting table `novels`. Table mapping: `novels`
- Capture and log of 5 kinesis events. 2 creating the `awsdms_apply_exceptions` table and 3 creating our tables.
- 4 inserts are made.
- Capture and log of 4 kinesis events. 2 on tables replicated by taks 1 and 2 on table replicated by taks 2.
- 3 table alterations are made. 1 per table.
- Capture and log of 3 kinesis events.
- Log of `table_statistics` for both tasks

 2 of them perform a Full Load replication on an dockered MariaDb. While the 2 others perform a CDC replication on a MariaDb RDS Database.

All tasks have the same Kinesis Stream as a target.

The sample run can be excuted by installing the python requirements and running the `run`/`run-aws` command.

```
# to run against Localstack
make run

# to run against aws
make run-aws
```
