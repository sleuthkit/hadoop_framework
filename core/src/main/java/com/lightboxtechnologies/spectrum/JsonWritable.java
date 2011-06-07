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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import org.codehaus.jackson.map.ObjectMapper;

public class JsonWritable implements Writable {
  private Object Data;
  private String Json;

  public void set(List l) {
    Data = l;
    Json = null;
  }

  public void set(Map<String,?> m) {
    Data = m;
    Json = null;
  }

  public Object get() {
    try {
      return getData();
    }
    catch (IOException e) {
      throw new RuntimeException("Bad JSON: \"" + Json + '"', e);
    }
  }

  public void readFields(DataInput in) throws IOException {
    Data = null;
    Json = in.readUTF();
  }

  private static final ObjectMapper mapper = new ObjectMapper();

  public void write(DataOutput out) throws IOException {
    out.writeUTF(getJSON());
  }

  private String getJSON() throws IOException {
    String ret;

    if (Json == null) {
      // cache serialized form
      ret = Data != null ? (Json = mapper.writeValueAsString(Data)) : "";
    }
    else {
      ret = Json;
    }

    return ret;
  }

  private Object getData() throws IOException {
    // cache unserialized form
    if (Data == null) {
      Data = mapper.readValue(Json, Object.class);
    }
    return Data;
  }

  public String toString() {
    try {    
      return getJSON();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
