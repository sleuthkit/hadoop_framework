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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
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

public class FolderCount {

  public static class FolderCountMapper
       extends Mapper<Text, FsEntry, Text, IntWritable>{

    private final Text Folder = new Text();
    private final IntWritable One = new IntWritable(1);

    @Override
    public void map(Text key, FsEntry value, Context context) throws IOException, InterruptedException {
      Folder.set((String)value.get("path"));
      context.write(Folder, One);
    }
  }

  static String convertScanToString(Scan scan) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    DataOutputStream dos = null;
    try {
      dos = new DataOutputStream(out);
      scan.write(dos);
      dos.close();
    }
    finally {
      IOUtils.closeQuietly(dos);
    }

    return Base64.encodeBytes(out.toByteArray());
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = new Configuration();
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
    scan.addFamily(Bytes.toBytes("core"));
    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, otherArgs[0]);
    job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
