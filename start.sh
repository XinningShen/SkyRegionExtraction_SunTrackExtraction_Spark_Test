#!/bin/bash
# A wrapper that executes:
# 1. Get DeviceID and three DATES before today(e.g. 2015-08-01, 2015-08-02, 2-015-08-03)
# 2. Pull the Images into seperate folder in local disk from rethinkdb
# 3. Tar the folder
# 4. Convert tar-file to hadoop .seq file using tar-to-seq.jar (e.g. java -jar tar-to-seq.jar data.tar input.seq). Hadoop SequenceFiles are flat files consisting of binary key/value pairs. For image, key is the image file name, value is the image content.
# 5. Run Spark (spark-submit application)


Main_Path=/Users/Sidney/Documents/workspace/ImageProcess_Spark_Test
SPARK_HOME=/Users/Sidney/Downloads/spark-1.4.1
ImagePullerProgram=ImagePullerManager.py
MainProgram=Main.py

# device_id=04E6768323EE
# device_id=04E676832123
# device_id=04E6768320AD
device_id=04E67693D76A

date1=`date -v-3d +%F`	# Three days ago
date2=`date -v-2d +%F`	# Two days ago
date3=`date -v-1d +%F`	# Yesterday
date_array=($date1 $date2 $date3)

time_start=09:00AM
time_end=07:00PM

time_start_hr=09
time_start_min=00
time_end_hr=19
time_end_min=00

count=0
declare -a seqFileArray

for date in ${date_array[@]}
do
	echo $date
	python ${Main_Path}/${ImagePullerProgram} ${device_id} ${date} ${time_start} ${date} ${time_end} ${Main_Path}
	date_cutoff=${date:5}
	dir_name="${device_id}${date_cutoff}_${time_start_hr}-${time_start_min}_to_${date_cutoff}_${time_end_hr}-${time_end_min}"
	tar -cvf "${dir_name}.tar" "${dir_name}/"
	java -jar "${Main_Path}/tar-to-seq.jar" "${dir_name}.tar" "${dir_name}.seq"
	seqFileArray[$count]=${dir_name}
	rm -rf "${dir_name}/"
	rm -rf "${dir_name}.tar"
	count=$(( $count + 1 ))
done

cd ${SPARK_HOME}
./bin/spark-submit ${Main_Path}/${MainProgram} ${Main_Path}/${seqFileArray[0]}.seq ${Main_Path}/${seqFileArray[1]}.seq ${Main_Path}/${seqFileArray[2]}.seq

for name in ${seqFileArray[@]}
do
	rm -rf ${Main_Path}/${seqFileArray[0]}.seq
	rm -rf ${Main_Path}/${seqFileArray[1]}.seq
	rm -rf ${Main_Path}/${seqFileArray[2]}.seq
done
