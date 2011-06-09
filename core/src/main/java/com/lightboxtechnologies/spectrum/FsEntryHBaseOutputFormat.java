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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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

import org.codehaus.jackson.map.ObjectMapper;

import java.util.Date;
import java.util.Map;
import java.util.List;
import java.io.IOException;

public class FsEntryHBaseOutputFormat extends OutputFormat {

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
    final String tblName = conf.get(HBaseTables.ENTRIES_TBL_VAR, HBaseTables.ENTRIES_TBL);
    final HBaseAdmin admin = new HBaseAdmin(hconf);
    if (!admin.tableExists(tblName)) {
    	final HTableDescriptor tableDesc = new HTableDescriptor(tblName);
// FIXME: how could the family already exist if the table doesn't?
    	if (!tableDesc.hasFamily(colFam)) {
    	  final HColumnDescriptor colFamDesc = new HColumnDescriptor(HBaseTables.ENTRIES_COLFAM);
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

  public RecordWriter<ImmutableBytesWritable, FsEntry> getRecordWriter(TaskAttemptContext ctx) throws IOException {
    return new FsEntryHBaseWriter(getHTable(ctx, HBaseTables.ENTRIES_COLFAM_B), HBaseTables.ENTRIES_COLFAM_B);
  }

  public static class FsEntryHBaseWriter extends RecordWriter<ImmutableBytesWritable, FsEntry> {
    private HTable Table;
    final private byte[] ColFam;

    FsEntryHBaseWriter(HTable tbl, byte[] colFam) {
      Table = tbl;
      ColFam = colFam;
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void addToPut(Put p, Map<String,?> map, byte[] colFam) {
      // should this be in the -Common class?
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
            binVal = Bytes.toBytes(((Number)val).longValue());
            break;
          case FsEntryHBaseCommon.DATE:
            binVal = Bytes.toBytes(((Date)val).getTime());
            break;
          case FsEntryHBaseCommon.JSON:
            try {
              binVal = mapper.writeValueAsBytes(val);
            }
            catch (IOException e) {
              throw new RuntimeException("Failed to serialize to JSON: " + val);
            }
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

    public static byte[] getBytes(ImmutableBytesWritable bw) {
      return bw.get();
    }

    public static Put createPut(byte[] key, Map<String,Object> map, byte[] colFam) {
      final Put p = new Put(key);
      addToPut(p, map, colFam);
      return p;
    }

    public static Put createPut(ImmutableBytesWritable key, Map<String,Object> map, byte[] colFam) {
      return createPut(getBytes(key), map, colFam);
    }

    @SuppressWarnings("unchecked")
    public static Put createPut(byte[] key, FsEntry entry, byte[] colFam) {
      final Put p = new Put(key);

      // FIXME: this logic should really be in FsEntry proper
      final Object o = entry.get("attrs");
      if (o != null && null == entry.getStreams().get("Content")) {
        String data = null;
        for (Map<String,Object> attr : (List<Map<String,Object>>)o) {
          final Number flags = (Number) attr.get("flags"),
                       type  = (Number) attr.get("type");
          final String name = (String) attr.get("name");
          if (flags != null && (flags.longValue() & 0x04) != 0
            && type != null && (type.longValue() & 0x80) != 0
            && name != null && name.equals("$Data"))
          {
            data = (String) attr.get("rd_buf");
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

      addToPut(p, entry.getChangedItems(), colFam);
      // addToPut(p, entry.getStreams(), colFam);

      return p;
    }

    public void write(ImmutableBytesWritable key, FsEntry entry) throws IOException {
      final Put p = createPut(getBytes(key), entry, ColFam);
      if (!p.isEmpty()) {
        Table.put(p);
      }
    }

    public void close(TaskAttemptContext ctx) {
      Table = null;
    }
  }
}
