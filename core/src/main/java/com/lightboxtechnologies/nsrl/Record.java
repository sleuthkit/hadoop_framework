package com.lightboxtechnologies.nsrl;

import java.util.Map;

/**
 * A utility class for NSRL records.
 *
 * @author Joel Uckelman
 */
class Record {
  protected Record() {}

  public static <K,V> void put(K key, V value, Map<K,V> map)
                                                      throws BadDataException {
    final V old = map.put(key, value);
    if (old != null) {
      // duplicate key
      throw new BadDataException("duplicate key: " + key);
    }
  }
}
