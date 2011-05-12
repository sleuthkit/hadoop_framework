package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

/**
 * @author Joel Uckelman
 */
public class OpenCSVLineTokenizerTest {
  private static final String[][] rows = {
    new String[] { "foo",  "bar,baz", "1" },
    new String[] { "xxx",  "yyy",     "2" },
    new String[] { "aaa",  "bbb",     "3" }
  };

  protected String buildCSV(String[] row) {
    return String.format("\"%s\",\"%s\",%s", (Object[]) row);
  }

  @Test
  public void parseTest() throws IOException {
    final LineTokenizer tok = new OpenCSVLineTokenizer();

    for (String[] row : rows) {
      assertArrayEquals(row, tok.tokenize(buildCSV(row)));
    }
  }
}
