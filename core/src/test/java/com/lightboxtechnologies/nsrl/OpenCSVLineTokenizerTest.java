/*
Copyright 2011, Lightbox Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
