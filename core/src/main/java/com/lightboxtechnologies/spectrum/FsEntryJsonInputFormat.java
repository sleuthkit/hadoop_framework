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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.io.IOException;

import org.json.simple.JSONAware;

public class FsEntryJsonInputFormat extends FileInputFormat implements Configurable {

  private Configuration    Conf;

  public Configuration getConf() {
    return Conf;
  }

  public void setConf(Configuration c) {
    Conf = c;
  }

  public RecordReader<Text, FsEntry> createRecordReader(InputSplit split, TaskAttemptContext ctx) throws IOException {
    return new FsEntryRecordReader();
  }

  public static class FsEntryRecordReader extends RecordReader<Text, FsEntry> {
    private final LineRecordReader Liner;
    private final Text Key;
    private Text Line;
    private final FsEntry Value;

    public FsEntryRecordReader() {
      Liner = new LineRecordReader();
      Key = new Text();
      Line = new Text();
      Value = new FsEntry();
    }

    public void close() throws IOException {
      Liner.close();
    }

    public float getProgress() {
      return Liner.getProgress();
    }

    public void initialize(InputSplit split, TaskAttemptContext ctx) throws IOException {
      Liner.initialize(split, ctx);
    }

    public boolean nextKeyValue() throws IOException {
      if (Liner.nextKeyValue()) {
        Line = Liner.getCurrentValue();
        Value.parseJson(Line.toString());
        Key.set(Value.getID());
        return true;
      }
      else {
        return false;
      }
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
      return Key;
    }

    public FsEntry getCurrentValue() throws IOException, InterruptedException {
      return Value;
    }
  }
}
