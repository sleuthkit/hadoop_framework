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
