#!/bin/sh

#set -x

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

export LD_LIBRARY_PATH=/home/uckelman/projects/lightbox/fsrip/deps/lib
FSRIP=/home/uckelman/projects/lightbox/fsrip/build/src/fsrip

HADOOP=/usr/bin/hadoop

JarFile=`ls -r $JarDir/sleuthkit-pipeline-r*-job.jar | head -n 1`
if [ $? -ne 0 ]; then
  echo "failed to find pipeline JAR"
  exit 1
fi

JsonFile=$FriendlyName.json
HdfsImage=$FriendlyName.dd

echo "jar file is ${JarFile}"

# rip filesystem metadata, upload to hdfs
$FSRIP dumpfs $ImagePath | $HADOOP jar $JarFile com.lightboxtechnologies.spectrum.Uploader $JsonFile
if [ $? -ne 0 ]; then
  echo "image metadata upload failed"
  exit 1
fi
echo "done uploading metadata"

# upload image to hdfs
ImageID=`$FSRIP dumpimg $ImagePath | $HADOOP jar $JarFile com.lightboxtechnologies.spectrum.Uploader $HdfsImage`
if [ $? -ne 0 ]; then
  echo "image upload failed"
  exit 1
fi
echo "done uploading image"

if [ $# -eq 4 ]; then
  ImageID=$4
fi
echo "Image ID is ${ImageID}"

# rip image info, insert in hbase
$FSRIP info $ImagePath | $HADOOP jar $JarFile com.lightboxtechnologies.spectrum.InfoPutter $ImageID $FriendlyName
if [ $? -ne 0 ]; then
  echo "image info registration failed"
  exit 1
fi
echo "image info registered"

# kick off ingest
$HADOOP jar $JarFile org.sleuthkit.hadoop.pipeline.Ingest $ImageID $HdfsImage $JsonFile $FriendlyName
if [ $? -ne 0 ]; then
  echo "ingest failed"
  exit 1
fi
echo "done with ingest"

# copy reports template
$HADOOP fs -cp /texaspete/templates/reports /texaspete/data/$ImageID/
if [ $? -ne 0 ]; then
  echo "copying reports template failed"
  exit 1
fi
echo "reports template copied"

# kick off pipeline
$HADOOP jar $JarFile org.sleuthkit.hadoop.pipeline.Pipeline $ImageID $FriendlyName
if [ $? -ne 0 ]; then
  echo "pipeline failed"
  exit 1
fi
echo "pipeline completed"

date
