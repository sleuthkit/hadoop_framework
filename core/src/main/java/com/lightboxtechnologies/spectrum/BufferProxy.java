/*
src/com/lightboxtechnologies/spectrum/JsonImport.java

Created by Jon Stewart on 2010-04-02.
Copyright (c) 2010 Lightbox Technologies, Inc. All rights reserved.
*/

package com.lightboxtechnologies.spectrum;

import java.io.InputStream;
import java.io.ByteArrayInputStream;

import org.apache.hadoop.fs.FileSystem;

class BufferProxy implements StreamProxy {

  private final byte[] Buffer;

  public BufferProxy(byte[] buf) {
    Buffer = buf;
  }

  public byte[] getBuffer() {
    return Buffer;
  }

  public InputStream open(FileSystem fs) { // doesn't need FS
    return new ByteArrayInputStream(Buffer);
  }
}
