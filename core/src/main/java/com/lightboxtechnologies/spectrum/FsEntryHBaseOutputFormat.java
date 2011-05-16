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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.OutputCommitter;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.io.hfile.Compression;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.util.Date;
import java.util.Map;
import java.util.List;
import java.io.IOException;

import org.json.simple.JSONAware;

public class FsEntryHBaseOutputFormat extends OutputFormat {

  public static final String ENTRY_TABLE = "com.lbt.htable";

  public static class NullOutputCommitter extends OutputCommitter {
    public void 	abortTask(TaskAttemptContext taskContext) {}
    public void 	cleanupJob(JobContext jobContext) {}
    public void 	commitTask(TaskAttemptContext taskContext) {}
    public boolean 	needsTaskCommit(TaskAttemptContext taskContext) { return false; }
    public void 	setupJob(JobContext jobContext) {}
    public void 	setupTask(TaskAttemptContext taskContext) {}
  }

  public OutputCommitter getOutputCommitter(TaskAttemptContext ctx) {
    return new NullOutputCommitter();
  }

  public void	checkOutputSpecs(JobContext context) {}

  public static HTable getHTable(TaskAttemptContext ctx, byte[] colFam) throws IOException {
    final Configuration hconf = HBaseConfiguration.create();
    final Configuration conf = ctx.getConfiguration();
    final String tblName = conf.get(ENTRY_TABLE, "entries");
    final HBaseAdmin admin = new HBaseAdmin(hconf);
    if (!admin.tableExists(tblName)) {
    	final HTableDescriptor tableDesc = new HTableDescriptor(tblName);
    	if (!tableDesc.hasFamily(colFam)) {
    	  final HColumnDescriptor colFamDesc = new HColumnDescriptor("core");
    	  colFamDesc.setCompressionType(Compression.Algorithm.GZ);
    		tableDesc.addFamily(colFamDesc);
    	}
    	admin.createTable(tableDesc);
    } // else should check whether the column family creates
    else if (!admin.isTableEnabled(tblName)) {
    	admin.enableTable(tblName);
    }
    return new HTable(hconf, tblName);
  }

  public RecordWriter<Text, FsEntry> getRecordWriter(TaskAttemptContext ctx) throws IOException {
    final byte[] colFam = Bytes.toBytes("core");
    return new FsEntryHBaseWriter(getHTable(ctx, colFam), colFam);
  }

  public static class FsEntryHBaseWriter extends RecordWriter<Text, FsEntry> {
    private HTable Table;
    private byte[] ColFam;

    FsEntryHBaseWriter(HTable tbl, byte[] colFam) {
      Table = tbl;
      ColFam = colFam;
    }

    public static void addToPut(Put p, Map<String,?> map, byte[] colFam) {
      byte[] col = null,
             binVal = null;
      String key = null;
      Object val = null;

      for (Map.Entry<String,?> pair : map.entrySet()) {
        key = pair.getKey();
        val = pair.getValue();
        if (val == null) {
          throw new RuntimeException("val was null for key " + key);
        }

        col = FsEntryHBaseCommon.createColSpec(val, key);
        switch (col[0]) {
          case FsEntryHBaseCommon.STRING:
            binVal = Bytes.toBytes((String)val);
            break;
          case FsEntryHBaseCommon.LONG:
            binVal = Bytes.toBytes((Long)val);
            break;
          case FsEntryHBaseCommon.DATE:
            binVal = Bytes.toBytes(((Date)val).getTime());
            break;
          case FsEntryHBaseCommon.JSON:
            binVal = Bytes.toBytes(val.toString());
            break;
          case FsEntryHBaseCommon.BYTE_ARRAY:
            binVal = (byte[])val;
            break;
          case FsEntryHBaseCommon.BUFFER_STREAM:
            binVal = ((BufferProxy)val).getBuffer();
            break;
          case FsEntryHBaseCommon.FILE_STREAM:
            binVal = Bytes.toBytes(((FileProxy)val).getPath());
            break;
          default:
            throw new RuntimeException("Didn't get something that could be converted to a byte[], " + val.toString() + " for key " + key);
        }
        p.add(colFam, col, binVal);
      }
    }

    public static Put createPut(String key, Map<String,Object> map, byte[] colFam) {
      final Put p = new Put(Bytes.toBytes(key));
      addToPut(p, map, colFam);
      return p;
    }

    @SuppressWarnings("unchecked")
    public static Put createPut(String key, FsEntry entry, byte[] colFam) {
      final Put p = new Put(Bytes.toBytes(key));

      final Object o = entry.get("attrs");
      if (o != null && null == entry.getStreams().get("Content")) {
        String data = null;
        for (Map<String,Object> attr : (List<Map<String,Object>>)o) {
          Long flags = (Long)attr.get("flags"),
               type  = (Long)attr.get("type");
          String name = (String)attr.get("name");
          if (flags != null && (flags.longValue() & 0x04) != 0
            && type != null && (type.longValue() & 0x80) != 0
            && name != null && name.equals("$Data"))
          {
            data = (String)attr.get("rd_buf");
            break;
          }
        }

        if (data != null && data.length() > 0) {
          byte[] v;
          try {
            v = Hex.decodeHex(data.toCharArray());
          }
          catch (DecoderException e) {
            throw new IllegalArgumentException(e);
          }

          p.add(colFam, FsEntryHBaseCommon.createColSpec(FsEntryHBaseCommon.BUFFER_STREAM, "Content"), v);
        }
      }

      addToPut(p, entry, colFam);
      addToPut(p, entry.getStreams(), colFam);

      return p;
    }

    public void write(Text key, FsEntry entry) throws IOException {
      Table.put(createPut(key.toString(), entry, ColFam));
    }

    public void close(TaskAttemptContext ctx) {
      Table = null;
      ColFam = null;
    }
  }
}
