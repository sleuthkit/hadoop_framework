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

package com.lightboxtechnologies.nsrl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * An MR job to load NSRL hash data to HBase.
 *
 * @author Joel Uckelman
 */
public class HashLoader {
  static class HashLoaderMapper
            extends Mapper<LongWritable,Text,ImmutableBytesWritable,KeyValue> {

    private final LineTokenizer tok = new OpenCSVLineTokenizer();

    private final ErrorConsumer err = new ErrorConsumer() {
      public void consume(BadDataException e, long linenum) {
        System.err.println("malformed record, line " + linenum);
        e.printStackTrace();
      }
    };

    private final HashRecordProcessor h_proc = new HashRecordProcessor();

    private HashLoaderHelper hlh;

    @Override
    protected void setup(Context context) throws IOException,
                                                 InterruptedException {
      super.setup(context);

      final Configuration conf = context.getConfiguration();

      long timestamp;
      // ensure that all mappers have the same timestamp
      try {
        timestamp = Long.parseLong(conf.get("timestamp"));
      }
      catch (NumberFormatException e) {
        throw new RuntimeException(e);
      }

      // load data for the manufacturer, os, and product maps
      final String mfg_filename  = conf.get("mfg_filename");
      final String os_filename   = conf.get("os_filename");
      final String prod_filename = conf.get("prod_filename");

      final FileSystem fs = FileSystem.get(conf);

      final Map<String,MfgData> mfg = new HashMap<String,MfgData>();
      final Map<String,OSData> os = new HashMap<String,OSData>();
      final Map<Integer,List<ProdData>> prod =
        new HashMap<Integer,List<ProdData>>();

      SmallTableLoader.load(
        fs, mfg_filename, mfg, os_filename, os, prod_filename, prod, tok, err
      );

      hlh = new HashLoaderHelper(prod, mfg, os, timestamp);
    }

    @Override
    protected void map(LongWritable linenum, Text line, Context context)
                                     throws InterruptedException, IOException {
      if (line.toString().startsWith("\"SHA-1\"")) {
        // skip this line, it's the header which gives the column names
        return;
      }

      // parse the line into the hash data
      final String[] cols = tok.tokenize(line.toString());

      HashData hd = null;
      try {
        hd = h_proc.process(cols);
      }
      catch (BadDataException e) {
        err.consume(e, linenum.get());
        return;
      }

      // create one row keyed to each of the md5 and sha1 hashes
      hlh.writeRow(hd.md5,  hd, context);
      hlh.writeRow(hd.sha1, hd, context);
    }
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = new Configuration();

    final String[] otherArgs =
      new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 5) {
      System.err.println(
        "Usage: HashLoader <mfgfile> <osfile> <prodfile> <hashfile> <outpath>"
      );
      System.exit(2);
    }

    final String mfg_filename    = otherArgs[0];
    final String os_filename     = otherArgs[1];
    final String prod_filename   = otherArgs[2];
    final String hash_filename   = otherArgs[3];
    final String output_filename = otherArgs[4];

    conf.set("mfg_filename", mfg_filename);
    conf.set("os_filename", os_filename);
    conf.set("prod_filename", prod_filename);

    conf.setLong("timestamp", System.currentTimeMillis());

    final Job job = new Job(conf, "HashLoader");
    job.setJarByClass(HashLoader.class);
    job.setMapperClass(HashLoaderMapper.class);
    job.setReducerClass(KeyValueSortReducer.class);
    job.setNumReduceTasks(1);

    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(HFileOutputFormat.class);

    TextInputFormat.addInputPath(job, new Path(hash_filename));
    HFileOutputFormat.setOutputPath(job, new Path(output_filename));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
