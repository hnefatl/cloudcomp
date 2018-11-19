#!/usr/bin/env spark-submit

# RDD api: https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD

import boto3
import random
import re
import json
import pyspark
import psycopg2
import string

sc = pyspark.SparkContext("local", "WordLetterCount", pyFiles=[])
rds = boto3.client("rds")

def mapreduce(data):
    return data \
            .flatMap(lambda line: re.findall(r"\w+", line)) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda x, y: x + y) \
            .sortBy(lambda x: x[1], False)

def generate_random_instance_id():
    random_suffix = "".join(random.choices(string.ascii_lowercase, k=5))
    return f"group8-{random_suffix}"

def create_rds_instance(db_name, user, password, zone, instance_id):
    print("Creating RDS instance")
    rds.create_db_instance(
        DBName=db_name,
        DBInstanceIdentifier=instance_id,
        DBInstanceClass="db.m5.large",
        AllocatedStorage=20,  # Minimum storage is 20GB
        AvailabilityZone=zone,
        Engine="postgres",
        MasterUsername=user,
        MasterUserPassword=password,
    )

    print("Waiting for RDS instance")
    rds.get_waiter("db_instance_available").wait(DBInstanceIdentifier=instance_id)

def get_rds_instance_endpoint(instance_id):
    instances = rds.describe_db_instances(DBInstanceIdentifier=instance_id)["DBInstances"]
    for instance in instances:
        if instance["DBInstanceIdentifier"] == instance_id:
            return (instance["Endpoint"]["Address"], instance["Endpoint"]["Port"])
    raise RuntimeError(f"Instance not found in described instances: {instances}")

def delete_rds_instance(instance_id, wait=True):
    print("Deleting DB instance")
    rds.delete_db_instance(DBInstanceIdentifier=instance_id, SkipFinalSnapshot=True)
    if wait:
        rds.get_waiter("db_instance_deleted").wait(DBInstanceIdentifier=instance_id)
        print("RDS instance deleted")
    
def write_to_db(host, port, db_name, user, password, data):
    print(f"Connecting to {host}:{port}")
    conn = psycopg2.connect(f"host='{host}' port='{port}' dbname='{db_name}' user='{user}' password='{password}'")
    cursor = conn.cursor()
    print("Deleting old tables")
    cursor.execute("DROP TABLE IF EXISTS words_spark")
    cursor.execute("DROP TABLE IF EXISTS letters_spark")
    print("Creating new tables")
    cursor.execute("CREATE TABLE words_spark ( rank INTEGER PRIMARY KEY, word TEXT NOT NULL, category TEXT NOT NULL, frequency INTEGER NOT NULL )")
    cursor.execute("CREATE TABLE letters_spark ( rank INTEGER PRIMARY KEY, letter TEXT NOT NULL, category TEXT NOT NULL, frequency INTEGER NOT NULL )")


def write_to_rds(data):
    instance_id = generate_random_instance_id()
    #instance_id = "group8-twtit"
    db_name = "kc506_rc691_CloudComputingCoursework"
    user = "foo"
    password = "1234567890"
    zone = "eu-west-1a"
    try:
        create_rds_instance(db_name, user, password, zone, instance_id)

        host, port = get_rds_instance_endpoint(instance_id)
        write_to_db(host, port, db_name, user, password, None)

        delete_rds_instance(instance_id, wait=False)
    except rds.exceptions.DBInstanceAlreadyExistsFault:
        print("Instance already exists, deleting")
        delete_rds_instance(instance_id, wait=True)


def main():
    write_to_rds(None)
    #text_file = sc.textFile("test.txt")
    #counts = mapreduce(text_file)
    #counts.saveAsTextFile("output.txt")

if __name__ == "__main__":
    main()