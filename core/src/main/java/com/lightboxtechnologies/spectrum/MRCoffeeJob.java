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

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.commons.codec.DecoderException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lightboxtechnologies.io.IOUtils;

public class MRCoffeeJob {

  protected static class MRCoffeeMapper
    extends Mapper<ImmutableHexWritable,FsEntry,ImmutableHexWritable,JsonWritable> {

    private static final Log LOG =
      LogFactory.getLog(MRCoffeeMapper.class.getName());

    protected long timestamp;

//    protected FsEntryFilter filter = new AllFsEntryFilter();

    protected FsEntryFilter filter = new FsEntryFilter() {
      public boolean accept(byte[] id, FsEntry entry) {
/*
        final Object type = entry.get("type");
        LOG.info("type == " + type);
        if (type instanceof Number) {
          if (((Number) type).longValue() == 1)  {
            final Object name_type = entry.get("name_type");
            LOG.info("name_type == " + name_type);
            if (name_type instanceof Number) {
              if (((Number) name_type).longValue() == 5) {
                return true;
              }
            }
          }
        }
        return false;
*/
        final Object name_type = entry.get("name_type");
        LOG.info("name_type == " + name_type);
        if (name_type instanceof Number) {
          if (((Number) name_type).longValue() == 5) {
            return true;
          }
        }
        return false; 
      }
    };

    protected byte[] command;

    protected MRCoffeeClient client;

    protected final byte[] buf = new byte[4096];

    protected final JsonWritable json = new JsonWritable();

    protected String pipe_path = "/tmp";
    protected final String mrcoffee_path = "/tmp/mrcoffee";

    protected Process mrcoffee;

    @Override
    protected void setup(Context context)
                                     throws IOException, InterruptedException {
      LOG.info("Setup called");

      final Configuration conf = context.getConfiguration();

      // ensure that all mappers have the same timestamp
      try {
        timestamp = Long.parseLong(conf.get("timestamp"));
      }
      catch (NumberFormatException e) {
        throw new RuntimeException(e);
      }

      // construct the command byte array as a series of C-style strings
      final StringBuilder sb = new StringBuilder();
      for (String arg : conf.getStrings("command")) {
        sb.append(arg).append('\0');
      } 
      command = sb.toString().getBytes();

      // name pipe path after this job
      pipe_path +=
        '/' + context.getJobID().toString() + '_' + UUID.randomUUID();

      // delete old socket file, if it exists
      final File pipe_file = new File(pipe_path);
      pipe_file.delete();

      // start MRCoffee server
      final ProcessBuilder pb = new ProcessBuilder(mrcoffee_path, pipe_path);
      mrcoffee = pb.start();

      // give MRCoffee time to create a socket
      while (!pipe_file.exists()) {
        Thread.sleep(100);
      }

      // create a MRCoffee client
      client = new MRCoffeeClient();
      
      try {
        client.open(pipe_file);
      }
      catch (IOException e) {
        IOUtils.closeQuietly(client);
        throw (IOException) new IOException().initCause(e);
      }
    }

    @Override
    protected void map(ImmutableHexWritable key, FsEntry entry, Context context)
                                     throws IOException, InterruptedException {
      final String path = entry.fullPath();

      // check whether the filter accepts this entry
      if (!filter.accept(key.get(), entry)) {
        LOG.info("Skipping " + path);
        return;
      }

      LOG.info("Processing " + path);

      // try to get the size of this entry
      final Object o = entry.get("size");
      if (!(o instanceof Number)) {
        LOG.info("Stream length for " + path + " was " + o + ", not a number"); 
        return;
      }

      final long size = ((Number) o).longValue();
      if (size < 0) {
        LOG.info("Stream length for " + path + " was " + size);
        return;
      }   

      // feed the data to MRCoffee
      InputStream in = null;
      try {
// TODO: support operations on other streams?
        in = entry.getInputStream();
        if (in == null) {
          LOG.info("Stream for " + path + " was null");
          return; 
        }
        
        // send the command
        client.writeCommand(command);

        // send the data
        LOG.info("Streaming " + path + ", " + size + " bytes");
        client.writeLength(size);

        final OutputStream out = client.getOutputStream();
        IOUtils.copy(in, out, buf, size);
        out.flush();
        in.close();
      }
      finally {
        IOUtils.closeQuietly(in);
      }
 
      // get the result
      final MRCoffeeClient.Result result = client.readResult();
     
      // convert the result to JSON
      final Map<String,Object> map = new HashMap<String,Object>();
      map.put("stdout", result.stdout);
      map.put("stderr", result.stderr);
      json.set(map);

      // write the result
      context.write(key, json);

      LOG.info("Finished " + path);
    }

    @Override
    protected void cleanup(Context context) throws IOException {
      LOG.info("Cleanup called");

      client.close();

// TODO: do this more gracefully
      // shut down MRCoffee
      mrcoffee.destroy();

      // remove socket file
      new File(pipe_path).delete();
    }
  }

  public static int run(
      String imageID, String outpath, String[] command, Configuration conf)
                              throws ClassNotFoundException, DecoderException,
                                     IOException, InterruptedException
  {
    conf.setStrings("command", command);
    conf.setLong("timestamp", System.currentTimeMillis());

    final Job job = new Job(conf, "MRCoffeeJob");
    job.setJarByClass(MRCoffeeJob.class);

    job.setMapperClass(MRCoffeeMapper.class);

//    job.setReducerClass(KeyValueSortReducer.class);
//    job.setNumReduceTasks(1);
    job.setNumReduceTasks(0);

    FsEntryHBaseInputFormat.setupJob(job, imageID);
    job.setInputFormatClass(FsEntryHBaseInputFormat.class);

    job.setOutputKeyClass(ImmutableHexWritable.class);
//    job.setOutputValueClass(KeyValue.class);
    job.setOutputValueClass(JsonWritable.class);
//    job.setOutputFormatClass(HFileOutputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

//    HFileOutputFormat.setOutputPath(job, new Path(outpath));
    TextOutputFormat.setOutputPath(job, new Path(outpath));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args)
                               throws ClassNotFoundException, DecoderException,
                                      IOException, InterruptedException {
    final Configuration conf = HBaseConfiguration.create();
    final String[] otherArgs =
      new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length < 3) {
      System.err.println(
        "Usage: MRCoffeeJob <image_id> <outpath> <command>..."
      );
      System.exit(2);
    }

    // get command and arguments
    final String[] command = new String[otherArgs.length - 2];
    System.arraycopy(args, 2, command, 0, command.length);

    System.exit(run(args[0], args[1], command, conf));
  }
}
