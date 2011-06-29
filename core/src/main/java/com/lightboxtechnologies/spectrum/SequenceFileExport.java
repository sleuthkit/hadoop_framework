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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Hex;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public class SequenceFileExport {

  private static final Log LOG = LogFactory.getLog(SequenceFileExport.class);

  protected static class SequenceFileExportMapper extends
        Mapper<ImmutableHexWritable,FsEntry,BytesWritable,MapWritable> {

    private final Set<String> Extensions = new HashSet<String>();

    private final BytesWritable OutKey = new BytesWritable();
    private final MapWritable Fields = new MapWritable();
    private final Text FullPath = new Text();
    private final Text Ext = new Text();
    private final Text Sha = new Text();
    private final Text Md5 = new Text();
// FIXME: IBW instead?
    private final BytesWritable Vid = new BytesWritable();
    private final Text HdfsPath = new Text();

    public SequenceFileExportMapper() {
      Fields.put(new Text("full_path"), FullPath);
      Fields.put(new Text("extension"), Ext);
      Fields.put(new Text("sha1"), Sha);
      Fields.put(new Text("md5"), Md5);
      Fields.put(new Text("data"), Vid);
      Fields.put(new Text("hdfs_path"), HdfsPath);
    }

    @Override
    protected void setup(Context context)
                                     throws IOException, InterruptedException {
      super.setup(context);

      final Configuration conf = context.getConfiguration();
     
      // get permissible file extensions from the configuration 
      Extensions.clear();
      Extensions.addAll(conf.getStringCollection("extensions"));
    }

    void encodeHex(Text val, FsEntry entry, String field) {
      Object o = entry.get(field);
      if (o != null && o instanceof byte[]) {
        byte[] b = (byte[])o;
        val.set(new String(Hex.encodeHex(b)));
      }
      else {
        LOG.warn(entry.fullPath() + " didn't have a hash for " + field);
        val.set("");
      }
    }

    @Override
    public void map(ImmutableHexWritable key, FsEntry value, Context context)
                                     throws IOException, InterruptedException {
      if (Extensions.contains(value.extension())) {
        FullPath.set(value.fullPath());
        Ext.set(value.extension());

        encodeHex(Sha, value, "sha1");
        encodeHex(Md5, value, "md5");

        if (value.isContentHDFS()) {
          Vid.setSize(0);
          HdfsPath.set(value.getContentHdfsPath());
        }
        else {
          final byte[] buf = value.getContentBuffer();
          if (buf == null) {
            LOG.warn(value.fullPath() + " didn't have a content buffer, skipping.");
            return;
          }
          Vid.set(buf, 0, buf.length);
          HdfsPath.set("");
        }
        byte[] keybytes = key.get();
        OutKey.set(keybytes, 0, keybytes.length);
        context.write(OutKey, Fields);
      }
    }
  }

  protected static void die() {
    System.err.println(
      "Usage: SequenceFileExport <image_id> <outpath> <ext> [<ext>]...\n" +
      "       SequenceFileExport -f <ext_file> <image_id> <outpath>"
    );
    System.exit(2);
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = new Configuration();

    final String[] otherArgs =
      new GenericOptionsParser(conf, args).getRemainingArgs();

    String imageID;
    String outpath;
    final Set<String> exts = new HashSet<String>();

    if ("-f".equals(otherArgs[0])) {
      if (otherArgs.length != 4) {
        die();
      }

      // load extensions from file
      final Path extpath = new Path(otherArgs[1]);

      InputStream in = null;
      try {
        in = extpath.getFileSystem(conf).open(extpath);

        Reader r = null;
        try {
          r = new InputStreamReader(in);

          BufferedReader br = null;
          try {
            br = new BufferedReader(r);

            String line;
            while ((line = br.readLine()) != null) {
              exts.add(line.trim().toLowerCase());
            }

            br.close();
          }
          finally {
            IOUtils.closeQuietly(br);
          }

          r.close();
        }
        finally {
          IOUtils.closeQuietly(r);
        }

        in.close();
      }
      finally {
        IOUtils.closeQuietly(in);
      }

      imageID = otherArgs[2];
      outpath = otherArgs[3];
    }
    else {
      if (otherArgs.length < 3) {
        die();
      }

      // read extensions from trailing args
      imageID = otherArgs[0];
      outpath = otherArgs[1];

      // lowercase all file extensions
      for (int i = 2; i < otherArgs.length; ++i) {
        exts.add(otherArgs[i].toLowerCase());
      }
    }

    conf.setStrings("extensions", exts.toArray(new String[exts.size()]));

    final Job job = new Job(conf, "SequenceFileExport");
    job.setJarByClass(SequenceFileExport.class);
    job.setMapperClass(SequenceFileExportMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(MapWritable.class);

    job.setInputFormatClass(FsEntryHBaseInputFormat.class);
    FsEntryHBaseInputFormat.setupJob(job, imageID);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputCompressionType(
      job, SequenceFile.CompressionType.BLOCK
    );

    FileOutputFormat.setOutputPath(job, new Path(outpath));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
