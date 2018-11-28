import boto3


def delete_entire_rds_instance(region, instance_id):
    ec2 = boto3.resource("ec2", region_name=region)
    rds = boto3.client("rds", region_name=region)
    # Delete instance, wait for deletion
    resp = rds.delete_db_instance(
        DBInstanceIdentifier=instance_id, SkipFinalSnapshot=True
    )
    rds.get_waiter("db_instance_deleted").wait(DBInstanceIdentifier=instance_id)

    subnet_group = resp["DBInstance"]["DBSubnetGroup"]

    subnet_group_name = subnet_group["DBSubnetGroupName"]
    vpc_id = subnet_group["VpcId"]
    rds.delete_db_subnet_group(DBSubnetGroupName=subnet_group_name)
    # Deleting a VPC is a bitch
    vpc = ec2.Vpc(vpc_id)
    for ig in vpc.internet_gateways.all():
        ig.detach_from_vpc(VpcId=vpc_id)
        ig.delete()
    for rt in vpc.route_tables.all():
        for rta in rt.associations:
            if not rta.main:
                rta.delete()
    for sg in vpc.security_groups.all():
        if sg.group_name != "default":
            sg.delete()
    for subnet in vpc.subnets.all():
        for interface in subnet.network_interfaces.all():
            interface.delete()
        subnet.delete()
    vpc.delete()


# Create a vpc, subnets, subnet group, and RDS instance
def create_entire_rds_instance(region, instance_id, username, password):
    availability_zones = get_availability_zones(region)
    # Create a VPC with subnets for each availability zone in the region
    vpc_id = create_vpc(region, availability_zones)

    # Create a subnet group containing all the subnets
    subnet_group_name = f"{instance_id}.subnetgroup"
    create_subnet_group(region, vpc_id, subnet_group_name)

    # Create the instance
    create_instance(region, vpc_id, subnet_group_name, instance_id, username, password)


# Return the names of all availability zones in a region
def get_availability_zones(region):
    ec2 = boto3.client("ec2", region_name=region)
    zones = ec2.describe_availability_zones()["AvailabilityZones"]
    return [zone["ZoneName"] for zone in zones if zone["RegionName"] == region]


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
            {  # Allow http traffic to kubelet server
                "IpProtocol": "tcp",
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                "ToPort": 10255,
                "FromPort": 10255,
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

    rds.get_waiter("db_instance_available").wait(DBInstanceIdentifier=instance_id)


# Get the instance endpoint information
def get_instance_endpoint(region, instance_id):
    rds = boto3.client("rds", region_name=region)
    try:
        instances = rds.describe_db_instances(DBInstanceIdentifier=instance_id)[
            "DBInstances"
        ]
        for instance in instances:
            if instance_id == instance["DBInstanceIdentifier"]:
                return {
                    "host": instance["Endpoint"]["Address"],
                    "port": instance["Endpoint"]["Port"],
                    "vpc_id": instance["DBSubnetGroup"]["VpcId"],
                }
    except Exception:
        pass
    raise RuntimeError("No instance found")


def get_custom_security_group_id(region, vpc_id):
    ec2 = boto3.client("ec2", region_name=region)
    response = ec2.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": ["group8.sec"]},
        ]
    )
    sgs = list(group["GroupId"] for group in response["SecurityGroups"])
    if len(sgs) != 1:
        raise RuntimeError(
            f"Got {len(sgs)} security groups in {vpc_id} with name group8.sec, expected 1."
        )
    return sgs[0]
