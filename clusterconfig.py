import json

class ClusterConfig:
    def __init__(self,
        # Path to the AWS
        key_path,
        # ID of the security group to use: used for allowing SSH/kubernetes traffic.
        security_group_id,
        master_type="c4.large",
        master_ami="ami-013be31976ca2c322",
        slave_type="t2.small",
        slave_ami="ami-013be31976ca2c322",
        username="ec2-user",
        # Name of the AWS keypair to be used for SSHing into machines
        key_name="aws",
        init_slave_count=10,
    ):
        self.key_path = key_path
        self.security_group_id = security_group_id
        self.master_type = master_type
        self.master_ami = master_ami
        self.slave_type = slave_type
        self.slave_ami = slave_ami
        self.username = username
        self.key_name = key_name
        self.init_slave_count = init_slave_count

    def json_show(self):
        return json.dumps(self.__dict__, indent=4)

    def json_load(self, json_text):
        self.__dict__ = json.loads(json_text)

    def pretty_show(self):
        print("Key path: " + self.key_path)
        print("Security Group ID: " + self.security_group_id)
        print("Master Instance Type: " + self.master_type)
        print("Master AMI ID: " + self.master_ami)
        print("Slave Instance Type: " + self.slave_type)
        print("Slave AMI ID: " + self.slave_ami)
        print("AMI Username: " + self.username)
        print("Private Key Path: " + self.key_name)
        print("Initial Slave Count: " + str(self.init_slave_count))