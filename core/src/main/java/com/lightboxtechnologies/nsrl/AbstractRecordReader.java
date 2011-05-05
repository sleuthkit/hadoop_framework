package com.lightboxtechnologies.nsrl;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.commons.io.IOUtils;

/**
 * A base class for {@link RecordReaders}s.
 * 
 * @author Joel Uckelman
 */
abstract class AbstractRecordReader implements RecordReader {
  public void read(InputStream in, RecordProcessor processor)
                                                           throws IOException {
    Reader r = null;
    try {
      r = new InputStreamReader(in);
      read(r, processor);
      r.close();
    }
    finally {
      IOUtils.closeQuietly(r);
    }
  }

  public void read(String filename, RecordProcessor processor)
                                                           throws IOException {
    Reader r = null;
    try {
      r = new FileReader(filename);
      read(r, processor);
      r.close();
    }
    finally {
      IOUtils.closeQuietly(r);
    }
  }
}
