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

import java.util.HashMap;
import java.util.Map;

/**
 * @author Joel Uckelman
 */
public class RecordTest {
  @Test
  public void putUnique() throws BadDataException {
    final Map<String,String> map = new HashMap<String,String>();
    final String key = "Bernadotte";
    final String val = "marshal of France";

    Record.put(key, val, map);
    assertEquals(1, map.size());
    assertEquals(val, map.get(key));
  }

  @Test(expected=BadDataException.class)
  public void putDuplicate() throws BadDataException {
    final Map<String,String> map = new HashMap<String,String>();
    final String key = "Bernadotte";
    final String val1 = "marshal of France";
    final String val2 = "king of Sweeden";

    Record.put(key, val1, map);
    Record.put(key, val2, map);
  }
}
