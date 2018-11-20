import boto3
import pymysql
import contextlib


# Creates a vpc with all the required gadgets, with subnets in each of the given availability zones.
# Deleting a vpc and all the created resources should be done from the VPC section of the AWS console.
def create_vpc(region, availability_zones):
    ec2 = boto3.resource("ec2", region_name=region)

    # VPC using some default 16-bit internal network
    vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
    vpc.wait_until_available()
    vpc.modify_attribute(EnableDnsSupport={"Value": True})
    vpc.modify_attribute(EnableDnsHostnames={"Value": True})

    # Internet gateway for outbound routing
    gateway = ec2.create_internet_gateway()
    vpc.attach_internet_gateway(InternetGatewayId=gateway.id)

    # Route table to route outbound traffic: route any underspecified traffic to the gateway
    for route_table in vpc.route_tables.all():
        route_table.create_route(DestinationCidrBlock="0.0.0.0/0", GatewayId=gateway.id)

    # Subnets in different availability zones with different IP addresses, linked to a VPC and the route_table
    for i, zone in enumerate(availability_zones):
        subnet = vpc.create_subnet(
            CidrBlock=f"10.0.{i}.0/24", VpcId=vpc.id, AvailabilityZone=zone
        )
        route_table.associate_with_subnet(SubnetId=subnet.id)

    # Security group to allow SQL traffic
    security = ec2.create_security_group(
        GroupName="group8.sec", Description="Group 8", VpcId=vpc.id
    )
    security.authorize_ingress(
        IpPermissions=[
            {  # Allow SQL traffic
                "IpProtocol": "tcp",
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                "ToPort": 3306,
                "FromPort": 3306,
            },
            {  # Allow any traffic from the same security group
                "IpProtocol": "-1",
                "UserIdGroupPairs": [
                    {"GroupId": str(security.id), "VpcId": str(vpc.id)}
                ],
            },
        ]
    )

    return vpc.id


def create_subnet_group(region, vpc_id, subnet_group_name):
    ec2 = boto3.resource("ec2", region_name=region)
    rds = boto3.client("rds", region_name=region)

    # Create an RDS subnet group containing the vpc subnets
    rds.create_db_subnet_group(
        DBSubnetGroupName=subnet_group_name,
        DBSubnetGroupDescription="Group 8",
        SubnetIds=[subnet.id for subnet in ec2.Vpc(vpc_id).subnets.all()],
    )


def create_instance(region, vpc_id, subnet_group_name, instance_id, username, password):
    ec2 = boto3.resource("ec2", region_name=region)
    rds = boto3.client("rds", region_name=region)

    rds.create_db_instance(
        DBInstanceIdentifier=instance_id,
        DBName="kc506_rc691_CloudComputingCoursework",
        DBInstanceClass="db.m5.large",
        AllocatedStorage=20,  # Minimum storage is 20GB
        Engine="mysql",
        MasterUsername=username,
        MasterUserPassword=password,
        VpcSecurityGroupIds=[sg.id for sg in ec2.Vpc(vpc_id).security_groups.all()],
        DBSubnetGroupName=subnet_group_name,
        BackupRetentionPeriod=0,
        PubliclyAccessible=True,
    )


# Get the instance endpoint information
def get_instance_endpoint(region, instance_id):
    rds = boto3.client("rds", region_name=region)
    instances = rds.describe_db_instances(DBInstanceIdentifier=instance_id)[
        "DBInstances"
    ]
    for instance in instances:
        if instance_id == instance["DBInstanceIdentifier"]:
            return (instance["Endpoint"]["Address"], instance["Endpoint"]["Port"])
    raise RuntimeError("No instance found")


# pymysql connections commit on close but don't actually close themselves: this context manager wraps one to provide
# sane behaviour.
class pymysql_connect:
    def __init__(self, *args, **kwargs):
        self._conn = pymysql.connect(*args, **kwargs)
        self._exitstack = contextlib.ExitStack()

    def __enter__(self):
        self._exitstack.enter_context(self._conn)
        return self._conn

    def __exit__(self, *_):
        self._exitstack.close()
        self._conn.close()

# Delete existing tables, create new tables
def initialise_instance(host, port, db_name, username, password):
    with pymysql_connect(
        host=host, port=port, user=username, password=password, db=db_name
    ) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS words_spark, letters_spark")
        cursor.execute(
            "CREATE TABLE words_spark ( rank INTEGER PRIMARY KEY, word TEXT NOT NULL, category TEXT NOT NULL, frequency INTEGER NOT NULL )"
        )
        cursor.execute(
            "CREATE TABLE letters_spark ( rank INTEGER PRIMARY KEY, letter TEXT NOT NULL, category TEXT NOT NULL, frequency INTEGER NOT NULL )"
        )