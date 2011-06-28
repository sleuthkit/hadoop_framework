/*
src/com/lightboxtechnologies/spectrum/ExtentsExtractor.java

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
import java.io.InputStream;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.sleuthkit.hadoop.SKJobFactory;
import org.sleuthkit.hadoop.SKMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExtentsExtractor {

  public static final Log LOG = LogFactory.getLog(ExtentsExtractor.class.getName());

  static class ExtentsExtractorMapper extends SKMapper<ImmutableHexWritable, FsEntry, LongWritable, JsonWritable> {

    final LongWritable Offset = new LongWritable();
    final Map<String,Object> Output = new HashMap<String,Object>();
    final List<Map<String,Object>> Extents =
      new ArrayList<Map<String,Object>>();
    final JsonWritable JsonOutput = new JsonWritable();

    public ExtentsExtractorMapper() {
      Output.put("extents", Extents);
    }

    static String errorString(ImmutableHexWritable key, FsEntry value, Exception e) {
      StringBuilder b = new StringBuilder("Exception on ");
      b.append(key.toString());
      b.append(":");
      b.append(value.fullPath());
      b.append(": ");
      b.append(e.toString());
      return b.toString();
    }

    boolean setExtents(List<Map<String,Object>> dataRuns, long fsByteOffset, long fsBlockSize) {
      Extents.clear();
      if (!dataRuns.isEmpty()) {
        for (Map<String,Object> run: dataRuns) {
          Map<String,Object> dr = new HashMap<String,Object>();
          dr.put("flags", run.get("flags"));
          dr.put("addr", (((Number)run.get("addr")).longValue() * fsBlockSize) + fsByteOffset);
          dr.put("offset", ((Number)run.get("offset")).longValue() * fsBlockSize);
          dr.put("len", ((Number)run.get("len")).longValue() * fsBlockSize);
          Extents.add(dr);
          return true;
        }
      }
      return false;
    }

    @Override
    public void map(ImmutableHexWritable key, FsEntry value, Context context) throws IOException, InterruptedException {
      try {
        final List<Map> attrs = (List<Map>)value.get("attrs");
        if (attrs == null) {
          LOG.info(value.fullPath() + " has no attributes");
          return;
        }
        long fsByteOffset = ((Number)value.get("fs_byte_offset")).longValue();
        long fsBlockSize = ((Number)value.get("fs_block_size")).longValue();
        for (Map attribute: attrs) {
          long flags = ((Number)attribute.get("flags")).longValue();
          long type = ((Number)attribute.get("type")).longValue();
          if ((flags & 0x03) > 0 && (type & 0x81) > 0) { // flags & type indicate this attribute has nonresident data
            Object nrds = attribute.get("nrd_runs");
            if (nrds == null) {
              LOG.warn(value.fullPath() + " had an nrd attr with null runs");
              return;
            }
            List<Map<String, Object>> runs = (List<Map<String,Object>>)nrds;
            if (setExtents(runs, fsByteOffset, fsBlockSize) && !Extents.isEmpty()) {
              Offset.set(((Number)Extents.get(0).get("addr")).longValue());
              Output.put("size", value.get("size"));
              Output.put("fp", value.fullPath());
              Output.put("id", key.toString());
              JsonOutput.set(Output);
              LOG.info(JsonOutput.toString());
              context.write(Offset, JsonOutput);
              break; // take the first one we get... FIXME someday
            }
          }
        }
      }
      catch (ClassCastException e) {
        LOG.warn(errorString(key, value, e));
        e.printStackTrace();
      }
      catch (NullPointerException e) {
        LOG.warn(errorString(key, value, e));
        e.printStackTrace();
      }
      catch (Exception e) {
        LOG.warn(errorString(key, value, e));
        e.printStackTrace();
      }
    }
  }

  public static void reportUsageAndExit() {
    System.err.println("Usage: ExtentsExtractor <imageID> <friendlyName> <sequenceFileNameHDFS>");
    System.exit(-1);
  }

  public static int run(String imageID, String friendlyName, String outDir) throws Exception {
    
    Job job = SKJobFactory.createJob(imageID, friendlyName, "ExtentsExtractor");
    
    job.setJarByClass(ExtentsExtractor.class);
    job.setMapperClass(ExtentsExtractorMapper.class);

    job.setNumReduceTasks(1);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(JsonWritable.class);
    job.setInputFormatClass(FsEntryHBaseInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, new Path(outDir));

    FsEntryHBaseInputFormat.setupJob(job, imageID);

    System.out.println("Spinning off ExtentsExtractor Job...");
    job.waitForCompletion(true);
    return 0;
  }

  public static void main (String[] argv) throws Exception { 
    run(argv[0], argv[1], argv[2]);
  }
}
