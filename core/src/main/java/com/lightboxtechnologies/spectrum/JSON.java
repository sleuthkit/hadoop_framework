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
