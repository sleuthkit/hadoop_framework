/*
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
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.sleuthkit.hadoop.SKMapper;
import org.sleuthkit.hadoop.SKJobFactory;

public class JsonImport {

/**
 * This is some documentation for FsEntryMapLoader
 */
  public static class FsEntryMapLoader
       extends SKMapper<Object, Text, ImmutableBytesWritable, FsEntry>{

    private final FsEntry Entry = new FsEntry();
    private final ImmutableBytesWritable Id =
      new ImmutableBytesWritable(new byte[FsEntryUtils.ID_LENGTH]);
    private final FsEntryUtils Helper = new FsEntryUtils();

    @Override
    protected void map(Object key, Text value, Context context)
                                     throws IOException, InterruptedException {
      if (Entry.parseJson(value.toString())) {
        // set the ID re-using the byte array in Id
        Helper.calcFsEntryID(Id.get(), getImageID(), (String)Entry.get("path"), (Integer)Entry.get("dirIndex"));

        context.write(Id, Entry);
      }
    }
  }

  public static int run(String jsonPath, String imageHash, String friendlyName, Configuration conf) throws Exception {
    if (conf == null) {
      conf = HBaseConfiguration.create();
    }
    conf.set(HBaseTables.ENTRIES_TBL_VAR, HBaseTables.ENTRIES_TBL);

    final Job job = SKJobFactory.createJobFromConf(imageHash, friendlyName, "JsonImport", conf);
    job.setJarByClass(JsonImport.class);
    job.setMapperClass(FsEntryMapLoader.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(FsEntryHBaseOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(jsonPath));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 3) {
      System.err.println("Usage: JsonImport <in> <image_hash> <friendly_name>");
      System.exit(2);
    }
    System.exit(run(args[0], args[1], args[2], conf));
  }
}
