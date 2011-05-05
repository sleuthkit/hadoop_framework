/*
src/com/lightboxtechnologies/spectrum/JsonImport.java

Created by Jon Stewart on 2010-04-02.
Copyright (c) 2010 Lightbox Technologies, Inc. All rights reserved.
*/

package com.lightboxtechnologies.spectrum;

import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class FileProxy implements StreamProxy {
  private final String FilePath;

  public FileProxy(String path) {
    FilePath = path;
  }

  public String getPath() {
    return FilePath;
  }

  public InputStream open(FileSystem fs) throws IOException {
    return fs.open(new Path(FilePath));
  }
}
