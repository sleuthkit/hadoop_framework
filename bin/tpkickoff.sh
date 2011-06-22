#!/bin/sh

if [ $# -ne 3 ]
  then
  echo "Usage: tpkickoff.sh image_friendly_name image_path jar_dir"
  exit 1
fi

pwd
date

FriendlyName=$1
ImagePath=$2
JarDir=$3

JarFile=`ls $JarDir/sleuthkit-pipeline-r*-job.jar | sort | tail -n 1`

JsonFile=$FriendlyName.json
HdfsImage=$FriendlyName.dd

echo "jar file is ${JarFile}"

# rip filesystem metadata, upload to hdfs
fsrip dumpfs $ImagePath | $HADOOP_HOME/bin/hadoop jar $JarFile com.lightboxtechnologies.spectrum.Uploader $JsonFile

# upload image to hdfs
ImageID=`cat $ImagePath | $HADOOP_HOME/bin/hadoop jar $JarFile com.lightboxtechnologies.spectrum.Uploader $HdfsImage`

# kick off ingest
$HADOOP_HOME/bin/hadoop jar $JarFile org.sleuthkit.hadoop.pipeline.Ingest $ImageID $HdfsImage $JsonFile

# copy reports template
$HADOOP_HOME/bin/hadoop fs -cp /texaspete/templates/reports /texaspete/data/$ImageID/

# kick off pipeline
$HADOOP_HOME/bin/hadoop jar $JarFile org.sleuthkit.hadoop.pipeline.Pipeline $ImageID $FriendlyName

date
