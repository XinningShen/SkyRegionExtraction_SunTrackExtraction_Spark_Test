# SkyRegionExtraction_SunTrackExtraction_Spark_Test

## Environment Set Up 
1. Spark set up:

 [See the Official Documentation](http://spark.apache.org/docs/latest/)
 
 [Set up step](http://genomegeek.blogspot.com/2014/11/how-to-install-apache-spark-on-mac-os-x.html)

2. Other configuration:

 python 2.6+, OpenCV 2.4.11, NumPy, Pyplot, SciPy.

## Run Spark Test
1. Clone to local disk
2. Set "Main_Path" and "SPARK_HOME" variable to your environment setting.
3. Run start.sh

## Explaination
1. Download three-day image-stream to local disk. ImagePullerManager.py will pull image from RethinkDB.
2. Convert image-stream to Hadoop SequenceFile seperately using "tar-to-seq.jar".
3. Submit application to Spark and Run Spark

## Notes
1. The test application deploy spark in Standalone Mode
2. This is just a test application that runs in local machine.
3. Set up 4 cores to simulate 4 parallel threads running application.
4. Does not save RESULT of SKY_REGION_MASK and SUN_TRACK_COEFFICIENT to disk.
