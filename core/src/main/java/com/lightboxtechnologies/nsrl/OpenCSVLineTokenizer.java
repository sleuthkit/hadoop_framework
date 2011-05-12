package com.lightboxtechnologies.nsrl;

import java.io.IOException;

import au.com.bytecode.opencsv.CSVParser;

/**
 * A {@link LineTokenizer} for NSRL record lines using opencsv.
 *
 * @author Joel Uckelman
 */
class OpenCSVLineTokenizer implements LineTokenizer {
  private final CSVParser parser = new CSVParser(',', '"');

  public String[] tokenize(String line) throws IOException {
    return parser.parseLine(line);
  }
}
