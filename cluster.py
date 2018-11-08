import asyncio
import boto3
from botocore.exceptions import ClientError
import collections
import concurrent
import contextlib
import functools
import pathlib
import paramiko
import retrying
import typing
import time

import clusterconfig

s3 = boto3.resource("s3")
ec2 = boto3.resource("ec2")

def create_master(config):
    instances = ec2.create_instances(
        ImageId=config.master_ami,
        InstanceType=config.master_type,
        MinCount=1,
        MaxCount=1,
        KeyName=config.key_name,
        SecurityGroupIds=[config.security_group_id],
    )
    return instances[0]

def create_slaves(config, count):
    return ec2.create_instances(
        ImageId=config.slave_ami,
        InstanceType=config.slave_type,
        MinCount=count,
        MaxCount=count,
        KeyName=config.key_name,
        SecurityGroupIds=[config.security_group_id],
    )


class Cluster:
    def __init__(self, config):
        self._running = False
        self._config = config
        self._master = None
        self._slaves = []

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exargs):
        self.stop()

    def running(self):
        return self._running

    async def start(self):
        if self.running():
            raise RuntimeError("Cluster already started")
        self._master = create_master(self._config)
        self._slaves = create_slaves(self._config, self._config.init_slave_count)

        self._master.wait_until_running()
        self._master.load()  # Refresh attributes
        for slave in self._slaves:
            slave.wait_until_running()
            slave.load()
        self._running = True

        #TODO(kc506): SSH setup script

    def stop(self):
        if self._master is not None:
            self._master.terminate()
        for slave in self._slaves:
            slave.terminate()
        self._running = False

    def wait_until_terminated(self):
        self._master.wait_until_terminated()
        for slave in self._slaves:
            slave.wait_until_terminated()

    async def run_ssh_on_all(self, command):
        return await self._run_ssh(command, [self._master] + self._slaves)
    async def run_ssh_on_master(self, command):
        return await self._run_ssh(command, [self._master])
    async def run_ssh_on_slaves(self, command):
        return await self._run_ssh(command, self._slaves)
    async def _run_ssh(self, command, instances):
        loop = asyncio.get_running_loop()

        def perform_ssh(instanc):
            with self._open_ssh(instance) as client:
                _, stdout, stderr = client.exec_command(command)
                return (stdout.read().decode(), stderr.read().decode())

        futures = []
        # Run all SSH connections in parallel using a dedicated thread pool
        with concurrent.futures.ThreadPoolExecutor() as pool:
            for instance in instances:
                fut = loop.run_in_executor(pool, functools.partial(perform_ssh, instance))
                futures.append(fut)

        return await asyncio.gather(*futures)

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