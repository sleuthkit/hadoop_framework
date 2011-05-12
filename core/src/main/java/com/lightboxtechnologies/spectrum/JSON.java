package com.lightboxtechnologies.spectrum;

import org.json.simple.JSONObject;

/**
 * A JSON utility class.
 *
 * @author Joel Uckelman
 */
public class JSON {

  protected JSON() {}

  static class DataException extends Exception {
    private static final long serialVersionUID = 1L;

    public DataException(String msg) {
      super(msg);
    }
  }

  public static <T> T getAs(JSONObject json, String key, Class<T> type)
                                                         throws DataException {
    final Object o = getNonNull(json, key);

    if (!type.isInstance(o)) {
      throw new DataException(
        key + " == " + o + ", a " + o.getClass().getName() +
              ", expected a " + type.getName());
    }

    return type.cast(o);
  }

  public static Object getNonNull(JSONObject json, String key)
                                                         throws DataException {
    if (key == null) {
      throw new DataException("key is null");
    }

    if (!json.containsKey(key)) {
      throw new DataException(key + " does not exist in " + json);
    }

    final Object o = json.get(key);
    if (o == null) {
      throw new DataException("value for " + key + " is null");
    }

    return o;
  }
}
