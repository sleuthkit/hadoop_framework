package com.lightboxtechnologies.nsrl;

import java.io.IOException;

/**
 * The interface for handling one line of an NSRL record file.
 * 
 * @author Joel Uckelman
 */
public interface LineHandler {
  public void handle(String line, long linenum) throws IOException;
}
