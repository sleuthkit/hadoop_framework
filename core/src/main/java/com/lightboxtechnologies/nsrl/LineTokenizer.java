package com.lightboxtechnologies.nsrl;

import java.io.IOException;

/**
 * The interface for tokenizers of NSRL record lines.
 *
 * @author Joel Uckelman
 */
public interface LineTokenizer {
  public String[] tokenize(String line) throws IOException;
}
