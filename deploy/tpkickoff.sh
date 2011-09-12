#!/bin/sh

if [ $# -ne 3 ] && [ $# -ne 4 ]
then
  echo "Usage: tpkickoff.sh image_friendly_name image_path jar_dir"
  exit 1
fi

if [ $# -eq 4 ]
then
  if [ ${#4} -ne 32 ]
  then
    echo "Invalid Image ID: Must be 32 digits long"
    exit 1
  fi
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
echo "done uploading"
if [ $# -eq 4 ]
  then
  ImageID=$4
fi
echo "Image ID is ${ImageID}"

# kick off ingest
$HADOOP_HOME/bin/hadoop jar $JarFile org.sleuthkit.hadoop.pipeline.Ingest $ImageID $HdfsImage $JsonFile $FriendlyName
echo "done with ingest"

# copy reports template
$HADOOP_HOME/bin/hadoop fs -cp /texaspete/templates/reports /texaspete/data/$ImageID/

# kick off pipeline
$HADOOP_HOME/bin/hadoop jar $JarFile org.sleuthkit.hadoop.pipeline.Pipeline $ImageID $FriendlyName

date
