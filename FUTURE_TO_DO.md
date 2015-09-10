1. Launch a standalone cluster on Amazon EC2. Spark has spark-ec2 script located in Spark’s ec2 directory, allows you to launch, manage and shut down Spark clusters on Amazon EC2. ([Official Documentation](http://spark.apache.org/docs/latest/ec2-scripts.html#running-spark-on-ec2))
2. Store Hadoop SequenceFile data in HDFS Cluster or Other Database like HBASE.
	- Spark can create distributed datasets from any storage source supported by Hadoop, including your local file system, HDFS, Cassandra, HBase, Amazon S3, etc. Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.
	- If using a path on the local filesystem, the file must also be accessible at the same path on worker nodes. Either copy the file to all workers or use a network-mounted shared file system.
3. Configuration tips:
	- Pay attention to [NETWORK CONFIGURATION](https://spark.apache.org/docs/latest/security.html#configuring-ports-for-network-security). Ensure that master node could reach worker node without password using ssh in Standalone Mode
    - Deploy mode: Cluster OR Client.
4. Some reference on stackoverflow:
	- [Submitting jobs to Spark EC2 cluster remotely](http://stackoverflow.com/questions/28666183/submitting-jobs-to-spark-ec2-cluster-remotely?rq=1)
	- [http://stackoverflow.com/questions/28398933/how-to-connect-master-and-slaves-in-apache-spark-standalone-mode](How to connect master and slaves in Apache-Spark? (Standalone Mode))
	- [Spark Installation | Unable to start spark slaves - asking password](http://stackoverflow.com/questions/29005220/spark-installation-unable-to-start-spark-slaves-asking-password)
    - [Where to put "local" data files?](http://apache-spark-user-list.1001560.n3.nabble.com/Where-to-put-quot-local-quot-data-files-td132.html)





