package com.lightboxtechnologies.nsrl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

import au.com.bytecode.opencsv.CSVParser;

import com.lightboxtechnologies.io.IOUtils;

/**
 * A {@link RecordReader} for NSRL hash records using opencsv for tokenization.
 * 
 * @author Joel Uckelman
 */
class OpenCSVRecordReader extends AbstractRecordReader {
  private final ErrorConsumer err;

  public OpenCSVRecordReader() {
    this(new ErrorConsumer() {
      public void consume(BadDataException e, long line) {
        System.err.println("malformed record, line " + line);
        e.printStackTrace();
      }
    });
  }

  public OpenCSVRecordReader(ErrorConsumer err) {
    this.err = err;
  }

  public void read(Reader r, RecordProcessor processor) throws IOException {
    final CSVParser parser = new CSVParser(',', '"');

    BufferedReader br = null;
    try {
      br = new BufferedReader(r);

      // first line gives column names, skip it
      br.readLine(); 

      // process each line
      String line;
      long linenum = 0;
      String[] cols;
      while ((line = br.readLine()) != null) {
        ++linenum;
        cols = parser.parseLine(line);

        try {
          processor.process(cols);
        }
        catch (BadDataException e) {
          err.consume(e, linenum);
        }
      }
    }
    finally {
      IOUtils.closeQuietly(br);
    }
  }
}
