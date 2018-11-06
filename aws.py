#!/usr/bin/env python3

import boto3
from botocore.exceptions import ClientError
import collections
import contextlib
from enum import Enum
import pathlib
import paramiko
import retrying
import typing
import time


s3 = boto3.resource("s3")
ec2 = boto3.resource("ec2")


class ClusterConfig:
    def __init__(self,
        # Path to the AWS
        key_path,
        # ID of the security group to use: used for allowing SSH/kubernetes traffic.
        security_group_id,
        master_type="c4.large",
        master_image="ami-013be31976ca2c322",
        slave_type="t2.small",
        slave_image="ami-013be31976ca2c322",
        username="ec2-user",
        # Name of the AWS keypair to be used for SSHing into machines
        key_name="aws",
        init_slave_count=10,
    ):
        self.key_path = key_path
        self.security_group_id = security_group_id
        self.master_type = master_type
        self.master_image = master_image
        self.slave_type = slave_type
        self.slave_image = slave_image
        self.username = username
        self.key_name = key_name
        self.init_slave_count = init_slave_count


def create_master(config):
    instances = ec2.create_instances(
        ImageId=config.master_image,
        InstanceType=config.master_type,
        MinCount=1,
        MaxCount=1,
        KeyName=config.key_name,
        SecurityGroupIds=[config.security_group_id],
    )
    return instances[0]

def create_slaves(config, count):
    return ec2.create_instances(
        ImageId=config.slave_image,
        InstanceType=config.slave_type,
        MinCount=count,
        MaxCount=count,
        KeyName=config.key_name,
        SecurityGroupIds=[config.security_group_id],
    )


class Cluster:
    def __init__(self, config):
        self._config = config
        self._master = None
        self._slaves = []

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exargs):
        self.stop()

    def start(self):
        self._master = create_master(self._config)
        self._slaves = create_slaves(self._config, self._config.init_slave_count)

        self._master.wait_until_running()
        self._master.load()  # Refresh attributes
        for slave in self._slaves:
            slave.wait_until_running()
            slave.load()

    def stop(self):
        self._master.terminate()
        for slave in self._slaves:
            slave.terminate()

    def wait_until_terminated(self):
        self._master.wait_until_terminated()
        for slave in self._slaves:
            slave.wait_until_terminated()

    def run_ssh_on_all(self, command):
        return self._run_ssh(command, [self._master] + self._slaves)
    def run_ssh_on_master(self, command):
        return self._run_ssh(command, [self._master])
    def run_ssh_on_slaves(self, command):
        return self._run_ssh(command, self._slaves)
    def _run_ssh(self, command, instances):
        output = []
        for instance in instances:
            with self._open_ssh(instance) as client:
                _, stdout, stderr = client.exec_command(command)
                output.append((stdout.read().decode(), stderr.read().decode()))
        return output

    def run_script_on_all(self, script_path):
        return self._run_script(script_path, [self._master] + self._slaves)
    def run_script_on_master(self, script_path):
        return self._run_script(script_path, [self._master])
    def run_script_on_slaves(self, script_path):
        return self._run_script(script_path, self._slaves)
    def _run_script(self, script_path, instances):
        contents = None
        with open(script_path, "r") as f:
            contents = f.read()
        if contents is None:
            raise RuntimeError("Bad file read")

        outputs = []
        for instance in instances:
            with self._open_ssh(instance) as client:
                stdin, stdout, stderr = client.exec_command("bash")
                stdin.write(contents)
                stdin.write("\nexit\n")
                outputs.append((stdout.read().decode(), stderr.read().decode()))
        return outputs


    @retrying.retry(stop_max_attempt_number=3, wait_fixed=1000)
    def _open_ssh(self, instance):
        client = paramiko.client.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        instance.load()  # Refresh instance information
        client.connect(
            hostname=instance.public_dns_name,
            username=self._config.username,
            key_filename=self._config.key_path,
            timeout=15,
        )
        return contextlib.closing(client)


    def spawn_new_workers(self, count):
        new_slaves = create_slaves(self._config, self._config.init_slave_count)
        for slave in new_slaves:
            slave.wait_until_running()
        self._slaves += new_slaves


    def get_config(self):
        return self._config