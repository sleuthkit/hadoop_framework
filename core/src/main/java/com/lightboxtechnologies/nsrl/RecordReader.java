package com.lightboxtechnologies.nsrl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * The interface for readers of NSRL hash records.
 * 
 * @author Joel Uckelman
 */
public interface RecordReader {
  public void read(Reader r, RecordProcessor processor) throws IOException;

  public void read(InputStream in, RecordProcessor processor)
                                                            throws IOException;

  public void read(String filename, RecordProcessor processor)
                                                            throws IOException;
}
