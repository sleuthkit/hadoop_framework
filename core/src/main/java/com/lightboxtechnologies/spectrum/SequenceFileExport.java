/*
src/com/lightboxtechnologies/spectrum/JsonImport.java

Copyright 2011, Lightbox Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.lightboxtechnologies.spectrum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.commons.codec.binary.Hex;

public class SequenceFileExport {

  public static class SequenceFileExportMapper
       extends Mapper<ImmutableHexWritable, FsEntry, ImmutableHexWritable, MapWritable> {

    private final MapWritable Fields = new MapWritable();
    private final Text FullPath = new Text();
    private final Text Ext = new Text();
    private final Text Sha = new Text();
    private final Text Md5 = new Text();
    private final BytesWritable Vid = new BytesWritable();
    private final Text HdfsPath = new Text();

    SequenceFileExportMapper() {
      Fields.put(new Text("full_path"), FullPath);
      Fields.put(new Text("extension"), Ext);
      Fields.put(new Text("sha1"), Sha);
      Fields.put(new Text("md5"), Md5);
      Fields.put(new Text("data"), Vid);
      Fields.put(new Text("hdfs_path"), HdfsPath);
    }

    @Override
    public void map(ImmutableHexWritable key, FsEntry value, Context context) throws IOException, InterruptedException {
      // file exts: avi, mp4, mpeg
      FullPath.set(value.fullPath());
      Ext.set(value.extension());
      Sha.set(new String(Hex.encodeHex((byte[])value.get("sha1"))));
      Md5.set(new String(Hex.encodeHex((byte[])value.get("md5"))));
      if (value.isContentHDFS()) {
        Vid.setSize(0);
        HdfsPath.set(value.getContentHdfsPath());
      }
      else {
        byte[] buf = value.getContentBuffer();
        Vid.set(buf, 0, buf.length);
        HdfsPath.set("");
      }
      context.write(key, Fields);
    }
  }

  public static void main(String[] args) throws Exception {
/*    final Configuration conf = new Configuration();
    final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: FolderCount <table> <outpath>");
      System.exit(2);
    }

    final Job job = new Job(conf, "FolderCount");
    job.setJarByClass(FolderCount.class);
    job.setMapperClass(FolderCountMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(FsEntryHBaseInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    final Scan scan = new Scan();
    scan.addFamily(HBaseTables.ENTRIES_COLFAM_B);
    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, otherArgs[0]);
    job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);*/
  }
}
