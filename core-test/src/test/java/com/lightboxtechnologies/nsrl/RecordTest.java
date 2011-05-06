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
