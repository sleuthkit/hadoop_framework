/*
src/com/lightboxtechnologies/spectrum/FsEntryHBaseInputFormat.java

Created by Jon Stewart on 2010-02-27.
Copyright (c) 2010 Lightbox Technologies, Inc. All rights reserved.
*/

package com.lightboxtechnologies.spectrum;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.io.IOException;

import org.json.simple.JSONAware;

import com.lightboxtechnologies.io.IOUtils;

public class FsEntryHBaseInputFormat extends InputFormat implements Configurable {

  private TableInputFormat TblInput;
  private Configuration    Conf;

  public FsEntryHBaseInputFormat() {
    TblInput = new TableInputFormat();
  }

  public Configuration getConf() {
    return Conf;
  }

  public void setConf(Configuration c) {
    Conf = c;
    TblInput.setConf(Conf);
  }

  public List<InputSplit> getSplits(JobContext ctx) throws IOException {
    return TblInput.getSplits(ctx);
  }

  public RecordReader<Text, FsEntry> createRecordReader(InputSplit split, TaskAttemptContext ctx) throws IOException {
    RecordReader<Text,FsEntry> outer = null;
    RecordReader<ImmutableBytesWritable,Result> inner = null;
    try {
      inner = TblInput.createRecordReader(split, ctx);
      outer = new FsEntryRecordReader(inner);
      return outer;
    }
    finally {
      // make sure inner is closed if outer creation throws
      if (outer == null) {
        IOUtils.closeQuietly(inner);
      }
    }
  }

  public static class FsEntryRecordReader extends RecordReader<Text, FsEntry> {
    private final RecordReader<ImmutableBytesWritable, Result> TblReader;
    private Result Cur;
    private final Text Key;
    private final FsEntry Value;
    private final byte[] Family;

    public FsEntryRecordReader(RecordReader<ImmutableBytesWritable, Result> rr) {
      TblReader = rr;
      Key = new Text("");
      Value = new FsEntry();
      Family = Bytes.toBytes("core");
    }

    public void close() throws IOException {
      TblReader.close();
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
      Cur = TblReader.getCurrentValue();
      Key.set(Bytes.toString(Cur.getRow()));
      return Key;
    }

    public FsEntry getCurrentValue() throws IOException, InterruptedException {
      final Map<byte[], byte[]> family = Cur.getFamilyMap(Family);
      FsEntryHBaseCommon.populate(family, Value, Value.getStreams());
      return Value;
    }

    public float getProgress() throws IOException, InterruptedException {
      return TblReader.getProgress();
    }

    public void initialize(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
      TblReader.initialize(split, ctx);
      Value.setFileSystem(FileSystem.get(ctx.getConfiguration()));
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
      return TblReader.nextKeyValue();
    }
  }
}
