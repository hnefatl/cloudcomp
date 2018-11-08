import json

class ClusterConfig:
    def __init__(self, config_path=None):
        if config_path is not None:
            self.json_load_path(config_path)
        else:
            # Default values
            self.key_path = None
            self.security_group_id = None
            self.master_type = "c4.large"
            self.master_ami = "ami-013be31976ca2c322"
            self.slave_type = "t2.small"
            self.slave_ami = "ami-013be31976ca2c322"
            self.username = "ec2-user"
            self.aws_keypair_name = "aws"
            self.init_slave_count = 10

    def json_show(self):
        return json.dumps(self.__dict__, indent=4)

    def json_load_path(self, json_path):
        with open(json_path, mode="r+") as file:
            contents = file.read()
        self.json_load(contents)

    def json_load(self, json_text):
        self.__dict__ = json.loads(json_text)

    def pretty_show(self):
        print("Private Key Path: " + self.key_path)
        print("Security Group ID: " + self.security_group_id)
        print("Master Instance Type: " + self.master_type)
        print("Master AMI ID: " + self.master_ami)
        print("Slave Instance Type: " + self.slave_type)
        print("Slave AMI ID: " + self.slave_ami)
        print("AMI Username: " + self.username)
        print("AWS KeyPair Name: " + self.aws_keypair_name)
        print("Initial Slave Count: " + str(self.init_slave_count))