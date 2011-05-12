/*
src/com/lightboxtechnologies/spectrum/JsonImport.java

Created by Jon Stewart on 2010-01-17.
Copyright (c) 2010 Lightbox Technologies, Inc. All rights reserved.
*/

package com.lightboxtechnologies.spectrum;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JsonImport {

/**
 * This is some documentation for FsEntryMapLoader
 */
  public static class FsEntryMapLoader
       extends Mapper<Object, Text, Text, FsEntry>{

    private final FsEntry Entry = new FsEntry();
    private final Text Id = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
                                     throws IOException, InterruptedException {
      if (Entry.parseJson(value.toString())) {
        Id.set(Entry.getID());
        context.write(Id, Entry);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = new Configuration();
    final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: JsonImport <in> <table>");
      System.exit(2);
    }

    conf.set(FsEntryHBaseOutputFormat.ENTRY_TABLE, otherArgs[1]);
    final Job job = new Job(conf, "JsonImport");
    job.setJarByClass(JsonImport.class);
    job.setMapperClass(FsEntryMapLoader.class);
    // job.setCombinerClass(IntSumReducer.class);
    // job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(0);
    // job.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(FsEntryHBaseOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    // FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
