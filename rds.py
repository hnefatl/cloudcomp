import boto3
import psycopg2

class RDS:
    def __init__(self, zone, instance_id, db_name=None, user=None, password=None):
        self._rds = boto3.client("rds")
        self._zone = zone
        self._instance_id = instance_id
        self._db_name = "kc506_rc691_CloudComputingCoursework" if db_name is None else db_name
        self._user = "foo" if user is None else user
        self._pass = "1234567890" if password is None else password

    def create_instance(self):
        while True:
            try:
                self._rds.create_db_instance(
                    DBName=self._db_name,
                    DBInstanceIdentifier=self._instance_id,
                    DBInstanceClass="db.m5.large",
                    AllocatedStorage=20,  # Minimum storage is 20GB
                    AvailabilityZone=self._zone,
                    Engine="postgres",
                    MasterUsername=self._user,
                    MasterUserPassword=self._pass,
                )
                self._wait_for_instance_available()
                self._initialise_instance()
                return
            except self._rds.exceptions.DBInstanceAlreadyExistsFault:
                self.delete_instance(wait=True)

    def delete_instance(self, wait=True):
        self._rds.delete_db_instance(DBInstanceIdentifier=self._instance_id, SkipFinalSnapshot=True)
        if wait:
            self._wait_for_instance_deleted()

    # Get the instance endpoint information
    def get_instance_endpoint(self):
        instances = self._rds.describe_db_instances(DBInstanceIdentifier=self._instance_id)["DBInstances"]
        for instance in instances:
            if instance["DBInstanceIdentifier"] == self._instance_id:
                return (instance["Endpoint"]["Address"], instance["Endpoint"]["Port"])
        raise RuntimeError(f"Instance not found in described instances: {instances}")

    # Delete existing tables, create new tables
    def _initialise_instance(self):
        self._wait_for_instance_available()
        host, port = self.get_instance_endpoint()

        conn = psycopg2.connect(
            f"host='{host}' port='{port}' dbname='{self._db_name}' user='{self._user}' password='{self._pass}'"
        )
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS words_spark")
        cursor.execute("DROP TABLE IF EXISTS letters_spark")
        cursor.execute("CREATE TABLE words_spark ( rank INTEGER PRIMARY KEY, word TEXT NOT NULL, category TEXT NOT NULL, frequency INTEGER NOT NULL )")
        cursor.execute("CREATE TABLE letters_spark ( rank INTEGER PRIMARY KEY, letter TEXT NOT NULL, category TEXT NOT NULL, frequency INTEGER NOT NULL )")
        
    def _wait_for_instance_available(self):
        self._wait_for_instance_state("db_instance_available")

    def _wait_for_instance_deleted(self):
        self._wait_for_instance_state("db_instance_deleted")

    def _wait_for_instance_state(self, state):
        self._rds.get_waiter(state).wait(DBInstanceIdentifier=self._instance_id)