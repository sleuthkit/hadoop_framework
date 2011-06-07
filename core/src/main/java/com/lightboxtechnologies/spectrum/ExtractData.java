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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExtractData {
  protected ExtractData() {}

  public static final Log LOG = LogFactory.getLog(ExtractData.class.getName());

  public static void main(String[] args) throws Exception {
    final Configuration conf = new Configuration();
    final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 4) {
      System.err.println("Usage: ExtractData <table> <extents_file> <evidence file> <store_path>");
      System.exit(2);
    }

    final Job job = new Job(conf, "ExtractData");
    job.setJarByClass(ExtractData.class);
    job.setMapperClass(ExtractMapper.class);
    job.setReducerClass(KeyValueSortReducer.class);
    job.setNumReduceTasks(1);

    job.setInputFormatClass(RawFileInputFormat.class);
    RawFileInputFormat.addInputPath(job, new Path(otherArgs[2]));

    job.setOutputFormatClass(HFileOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);

    conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
    conf.set(HBaseTables.ENTRIES_TBL_VAR, otherArgs[0]);
    conf.set("com.lbt.storepath", otherArgs[3]);
    HFileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
    HBaseConfiguration.addHbaseResources(job.getConfiguration());

    final URI extents = new Path(otherArgs[1]).toUri();
    LOG.info("extents file is " + extents);

    DistributedCache.addCacheFile(extents, job.getConfiguration());
    conf.set("com.lbt.extentspath", extents.toString());
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
