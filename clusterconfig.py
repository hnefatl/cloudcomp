import json
import getpass


class ClusterConfig:
    def __init__(self):
        # Default values
        self.region = "eu-west-1"
        self.zone = "eu-west-1a"
        self.vpc_id = "vpc-0ba327852e691da2b"
        self.rds_host = "group8.cqnxkff6jrcr.eu-west-1.rds.amazonaws.com"
        self.rds_port = 3306
        self.rds_username = "foo"
        self.rds_password = "1234567890"
        self.master_type = "c4.large"
        self.slave_type = "t2.small"
        self.master_count = 1
        self.slave_count = 2
        self.cluster_name = "{}.group8.cluster.k8s.local".format(getpass.getuser())
        self.s3_bucket_prefix = "kubernetes.group8"

    def json_show(self):
        return json.dumps(self.__dict__, indent=4)

    def json_load_path(self, json_path):
        with open(json_path, mode="r+") as file:
            contents = file.read()
        self.json_load(contents)

    def json_load(self, json_text):
        self.__dict__ = json.loads(json_text)
