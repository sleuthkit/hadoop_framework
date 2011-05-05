/*
src/com/lightboxtechnologies/spectrum/JsonImport.java

Created by Jon Stewart on 2010-03-23.
Copyright (c) 2010 Lightbox Technologies, Inc. All rights reserved.
*/

package com.lightboxtechnologies.spectrum;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.NullWritable;

public class RawFileInputFormat extends FileInputFormat<NullWritable, FileSplit> {
  public static class RawFileRecordReader extends RecordReader<NullWritable, FileSplit> {
    private FileSplit Split;
    private boolean GetCalled = false;
    
    public void close() {}
    
    public NullWritable getCurrentKey() {
      return NullWritable.get();
    }

    public FileSplit getCurrentValue() {
      GetCalled = true;
      return Split;
    }

    public float getProgress() {
      return 0.0f;
    }

    public void initialize(InputSplit split, TaskAttemptContext ctx) {
      Split = (FileSplit)split;
    }

    public boolean nextKeyValue() {
      return !GetCalled;
    }
  }

  public RecordReader<NullWritable, FileSplit> createRecordReader(InputSplit split, TaskAttemptContext ctx) {
    return new RawFileRecordReader();
  }
}
