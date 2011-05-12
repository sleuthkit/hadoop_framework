package com.lightboxtechnologies.nsrl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.commons.io.IOUtils;

/**
 * A class which loads NSRL record files.
 *
 * @author Joel Uckelman
 */
public class RecordLoader {
  public void load(Reader r, LineHandler lh) throws IOException {
    BufferedReader br = null;
    try {
      br = new BufferedReader(r);

      // first line gives column names, skip it
      br.readLine();

      // process each line
      String line;
      long linenum = 0;
      while ((line = br.readLine()) != null) {
        ++linenum;
        lh.handle(line, linenum);
      }

      br.close();
    }
    finally {
      IOUtils.closeQuietly(br);
    }
  }

  public void load(InputStream in, LineHandler lh) throws IOException {
    Reader r = null;
    try {
      r = new InputStreamReader(in);
      load(r, lh);
      r.close();
    }
    finally {
      IOUtils.closeQuietly(r);
    }
  }

  public void load(String filename, LineHandler lh) throws IOException {
    Reader r = null;
    try {
      r = new FileReader(filename);
      load(r, lh);
      r.close();
    }
    finally {
      IOUtils.closeQuietly(r);
    }
  }
}
