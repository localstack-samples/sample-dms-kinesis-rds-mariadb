import json
import os
from typing import Iterable

import aws_cdk as cdk
from aws_cdk import (
    aws_iam as iam,
    aws_dms as dms,
    aws_ec2 as ec2,
    aws_kinesis as kinesis,
    aws_rds as rds,
    aws_secretsmanager as secretsmanager,
    SecretValue,
    Stack,
)
from constructs import Construct


DB_NAME = os.getenv("DB_NAME", "")

# Only used for creating endpoint to containered Mariadb
USER_PWD = os.getenv("USERPWD", "")
USERNAME = os.getenv("USERNAME", "")
DB_ENDPOINT = os.getenv("DB_ENDPOINT", "")
DB_PORT = os.getenv("DB_PORT", "")


class DmsSampleStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # VPC configuration
        vpc = ec2.Vpc(
            self,
            "dms-sample",
            vpc_name="dmsSample",
            create_internet_gateway=True,
            enable_dns_hostnames=True,
            enable_dns_support=True,
            nat_gateways=0,
        )
        security_group = create_security_group(self, vpc)

        # Assume Role for dms resources
        dms_assume_role = iam.Role(
            self,
            "dns-assume-role",
            assumed_by=iam.ServicePrincipal("dms.amazonaws.com").grant_principal,
        )

        # # Launching both databases and creating their Dms endpoint

        #  Creating an rds database with a secret managed secret
        db_cdc_instance = create_db_instance(self, "rds_instance", vpc, security_group)
        db_cdc_secret = db_cdc_instance.secret
        db_cdc_port_as_number = cdk.Token.as_number(
            db_cdc_instance.db_instance_endpoint_port
        )

        if DB_ENDPOINT and DB_PORT:
            # When running locally we won't create another rds instance.
            # We will simply generate a secret with our containered Mariadb
            db_full_secret = create_secret(self, "mariadb-access-secret")
            db_full_port_as_number = int(DB_PORT)
        else:
            # When deploying against aws, we will create a second rds database
            db_full_instance = create_db_instance(
                self, "full-load-instance", vpc, security_group
            )
            db_full_port_as_number = cdk.Token.as_number(
                db_full_instance.db_instance_endpoint_port
            )
            db_full_secret = db_full_instance.secret

        # Creating both source endpoints
        cdc_source_endpoint = create_source_endpoint(
            self, "cdc-endpoint", db_cdc_instance.engine.engine_type, db_cdc_secret
        )
        full_source_endpoint = create_source_endpoint(
            self, "full-endpoint", "mariadb", db_full_secret
        )

        # updating the security group to allow ingress to DB
        ports = [db_full_port_as_number, db_cdc_port_as_number]
        allow_from_ports(security_group, ports)

        # Creation of the kinesis Stream
        kinesis_stream = create_kinesis_stream(self, dms_assume_role)
        target_endpoint = create_kinesis_target_endpoint(
            self, kinesis_stream, dms_assume_role
        )

        # Creating a replication instance
        replication_instance = create_replication_instance(self, vpc, security_group)

        # Cdc task processing tables accounts and authors
        cdc_task_1 = create_replication_task(
            self,
            "cdc-task-1",
            replication_instance=replication_instance,
            source=cdc_source_endpoint,
            target=target_endpoint,
            migration_type="cdc",
            table_mappings={
                "rules": [
                    {
                        "rule-type": "selection",
                        "rule-id": "1",
                        "rule-name": "tables-a-to-m",
                        "object-locator": {
                            "schema-name": DB_NAME,
                            "table-name": "a%",
                        },
                        "rule-action": "include",
                    }
                ]
            },
        )

        # Cdc task processing table novels
        cdc_task_2 = create_replication_task(
            self,
            "cdc-task-2",
            replication_instance=replication_instance,
            source=cdc_source_endpoint,
            target=target_endpoint,
            migration_type="cdc",
            table_mappings={
                "rules": [
                    {
                        "rule-type": "selection",
                        "rule-id": "1",
                        "rule-name": "tables-n-to-z",
                        "object-locator": {
                            "schema-name": DB_NAME,
                            "table-name": "novels",
                        },
                        "rule-action": "include",
                    }
                ]
            },
        )

        # Full load task processing tables tables accounts and authors
        full_load_task_1 = create_replication_task(
            self,
            "full-load-task-1",
            replication_instance=replication_instance,
            source=full_source_endpoint,
            target=target_endpoint,
            migration_type="full-load",
            table_mappings={
                "rules": [
                    {
                        "rule-type": "selection",
                        "rule-id": "1",
                        "rule-name": "tables-a-to-m",
                        "object-locator": {
                            "schema-name": DB_NAME,
                            "table-name": "a%",
                        },
                        "rule-action": "include",
                    }
                ]
            },
        )

        # Full load task processing tables novels
        full_load_task_2 = create_replication_task(
            self,
            "full-load-task-2",
            replication_instance=replication_instance,
            source=full_source_endpoint,
            target=target_endpoint,
            migration_type="full-load",
            table_mappings={
                "rules": [
                    {
                        "rule-type": "selection",
                        "rule-id": "1",
                        "rule-name": "tables-n-to-z",
                        "object-locator": {
                            "schema-name": DB_NAME,
                            "table-name": "novels",
                        },
                        "rule-action": "include",
                    }
                ]
            },
        )

        cdk.CfnOutput(self, "cdcTaskSecret", value=db_cdc_secret.secret_full_arn)
        cdk.CfnOutput(self, "cdcTask1", value=cdc_task_1.ref)
        cdk.CfnOutput(self, "cdcTask2", value=cdc_task_2.ref)

        cdk.CfnOutput(self, "fullTaskSecret", value=db_full_secret.secret_full_arn)
        cdk.CfnOutput(self, "fullTask1", value=full_load_task_1.ref)
        cdk.CfnOutput(self, "fullTask2", value=full_load_task_2.ref)

        cdk.CfnOutput(self, "kinesisStream", value=kinesis_stream.stream_arn)


# DMS helper functions


def create_kinesis_target_endpoint(
    stack: Stack, target: kinesis.Stream, dms_assume_role: iam.Role
) -> dms.CfnEndpoint:
    return dms.CfnEndpoint(
        stack,
        "target",
        endpoint_type="target",
        engine_name="kinesis",
        kinesis_settings=dms.CfnEndpoint.KinesisSettingsProperty(
            stream_arn=target.stream_arn,
            message_format="json",
            service_access_role_arn=dms_assume_role.role_arn,
            include_control_details=True,
            include_null_and_empty=True,
            include_partition_value=True,
            include_table_alter_operations=True,
            include_transaction_details=True,
            partition_include_schema_table=True,
        ),
    )


def create_source_endpoint(
    stack: Stack,
    endpoint_id,
    engine,
    secret: secretsmanager.ISecret | secretsmanager.Secret,
) -> dms.CfnEndpoint:
    endpoint_role = iam.Role(
        stack,
        f"{endpoint_id}-secret-access-role",
        assumed_by=iam.ServicePrincipal(
            service=f"dms.{stack.region}.amazonaws.com"
        ).grant_principal,
        inline_policies={
            "allowSecrets": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=["secretsmanager:GetSecretValue"],
                        effect=iam.Effect.ALLOW,
                        resources=[secret.secret_full_arn],
                    )
                ]
            )
        },
    )
    source_endpoint = dms.CfnEndpoint(
        stack,
        endpoint_id,
        endpoint_type="source",
        engine_name=engine,
        my_sql_settings=dms.CfnEndpoint.MySqlSettingsProperty(
            secrets_manager_secret_id=secret.secret_full_arn,
            secrets_manager_access_role_arn=endpoint_role.role_arn,
        ),
    )
    return source_endpoint


def create_replication_instance(
    stack: Stack, vpc: ec2.Vpc, security_group: ec2.SecurityGroup
):
    subnets = vpc.public_subnets
    # Role definitions
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "dms.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    cdk.aws_iam.CfnRole(
        stack,
        "DmsVpcRole",
        managed_policy_arns=[
            "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole",
        ],
        assume_role_policy_document=assume_role_policy_document,
        role_name="dms-vpc-role",  # this exact name needs to be set
    )
    replication_subnet_group = cdk.aws_dms.CfnReplicationSubnetGroup(
        stack,
        "ReplSubnetGroup",
        replication_subnet_group_description="Replication Subnet Group for DMS test",
        subnet_ids=[subnet.subnet_id for subnet in subnets],
    )

    return dms.CfnReplicationInstance(
        stack,
        "replication-instance",
        replication_instance_class="dms.t2.micro",
        allocated_storage=5,
        replication_subnet_group_identifier=replication_subnet_group.ref,
        allow_major_version_upgrade=False,
        auto_minor_version_upgrade=False,
        multi_az=False,
        publicly_accessible=True,
        vpc_security_group_ids=[security_group.security_group_id],
        availability_zone=subnets[0].availability_zone,
    )


def create_replication_task(
    stack,
    id: str,
    replication_instance: dms.CfnReplicationInstance,
    source: dms.CfnEndpoint,
    target: dms.CfnEndpoint,
    migration_type: str = "cdc",
    table_mappings: dict = None,
    replication_task_settings: dict = None,
) -> dms.CfnReplicationTask:
    if not table_mappings:
        table_mappings = {
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": "1",
                    "rule-name": "rule1",
                    "object-locator": {"schema-name": DB_NAME, "table-name": "%"},
                    "rule-action": "include",
                }
            ]
        }
    if not replication_task_settings:
        replication_task_settings = {"Logging": {"EnableLogging": True}}
        if migration_type == "cdc":
            replication_task_settings["BeforeImageSettings"] = {
                "EnableBeforeImage": True,
                "FieldName": "before-image",
                "ColumnFilter": "all",  # pk-only will only report the e.g. "author_id": 1
            }

    return dms.CfnReplicationTask(
        stack,
        id,
        replication_task_identifier=id,
        migration_type=migration_type,
        replication_instance_arn=replication_instance.ref,
        source_endpoint_arn=source.ref,
        target_endpoint_arn=target.ref,
        table_mappings=json.dumps(table_mappings),
        replication_task_settings=json.dumps(replication_task_settings),
    )


# Kinesis Helper functions


def create_kinesis_stream(stack: Stack, dms_assume_role: iam.Role) -> kinesis.Stream:
    target_stream = kinesis.Stream(
        stack, "TargetStream", shard_count=1, retention_period=cdk.Duration.hours(24)
    )
    target_stream.grant_read_write(dms_assume_role)
    target_stream.apply_removal_policy(cdk.RemovalPolicy.DESTROY)
    return target_stream


# RDS helper functions


def create_db_instance(
    stack: Stack,
    instance_id: str,
    vpc: ec2.Vpc,
    security_group: ec2.SecurityGroup,
) -> rds.DatabaseInstance:
    db_parameters_config = {
        "binlog_checksum": "NONE",
        "binlog_row_image": "Full",
        "binlog_format": "ROW",
    }
    return rds.DatabaseInstance(
        stack,
        instance_id,
        engine=rds.DatabaseInstanceEngine.maria_db(
            version=rds.MariaDbEngineVersion.VER_10_11
        ),
        vpc=vpc,
        removal_policy=cdk.RemovalPolicy.DESTROY,
        database_name=DB_NAME,
        security_groups=[security_group],
        parameters=db_parameters_config,
        vpc_subnets=ec2.SubnetSelection(subnets=vpc.public_subnets),
        publicly_accessible=True,
    )


# Secrets Manager functions


def create_secret(stack: Stack, secret_id: str) -> secretsmanager.Secret:
    return secretsmanager.Secret(
        stack,
        secret_id,
        secret_object_value={
            "host": SecretValue.unsafe_plain_text(DB_ENDPOINT),
            "port": SecretValue.unsafe_plain_text(DB_PORT),
            "username": SecretValue.unsafe_plain_text(USERNAME),
            "password": SecretValue.unsafe_plain_text(USER_PWD),
            "dbname": SecretValue.unsafe_plain_text(DB_NAME),
        },
    )


# VPC Helper functions


def create_security_group(stack: Stack, vpc: ec2.Vpc) -> ec2.SecurityGroup:
    return ec2.SecurityGroup(
        stack,
        "sg",
        vpc=vpc,
        description="Security group for DMS sample",
        allow_all_outbound=True,
    )


def allow_from_ports(security_group: ec2.SecurityGroup, ports: Iterable):
    security_group.connections.allow_from(
        other=ec2.Peer.any_ipv4(),
        port_range=ec2.Port.tcp_range(min(ports), max(ports)),
    )
