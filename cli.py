#!/usr/bin/env python3

import aws

def main():
    config = aws.ClusterConfig(
        init_slave_count=1,
        key_path="/home/keith/.ssh/aws.pem",
        security_group_id="sg-09895ef455657c2e0",
    )

    print("Starting cluster")
    with aws.Cluster(config) as cluster:
        print("Cluster started")

        outputs = cluster.run_ssh_on_all("date && echo 'foo'")
        #outputs = cluster.run_script_on_all("/home/keith/modules/cloud/cloudcomp/demo.sh")
        for (out, _) in outputs:
            print(out)

        input("Enter to terminate")
        print("Stopping cluster")


if __name__ == "__main__":
    main()