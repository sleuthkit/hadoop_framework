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

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonWritableTest {
  @Test
  public void testSetList() throws IOException {
    final List<Number> expected = Arrays.asList(
      new Number[] {5, 89, 2458, 24, 64, 17, Long.MAX_VALUE }
    );

    final List<Number> input = new ArrayList<Number>(expected);
    assertEquals(expected, input);

    final JsonWritable w1 = new JsonWritable();
    w1.set(input);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    w1.write(new DataOutputStream(out));

    final JsonWritable w2 = new JsonWritable();
    w2.readFields(
      new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

    assertEquals(expected, w2.get());
  }

  @Test
  public void testSetMap() throws IOException {
    final Map<String,Object> expected = new HashMap<String,Object>();

    expected.put("three", 3);
    expected.put("hello", "world");
    expected.put("empty", new HashMap<Object,Object>());

    final Map<String,Object> input = new HashMap<String,Object>(expected);
    assertEquals(expected, input);

    final JsonWritable w1 = new JsonWritable();
    w1.set(input);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    w1.write(new DataOutputStream(out));

    final JsonWritable w2 = new JsonWritable();
    w2.readFields(
      new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

    assertEquals(expected, w2.get());
  }

  @Test
  public void testSecondSerialize() throws IOException {
    final Map<String,Object> m = new HashMap<String,Object>();

    m.put("three", 3);
    m.put("hello", "world");
    m.put("empty", new HashMap<Object,Object>());

    final JsonWritable w = new JsonWritable();
    w.set(m);
    assertEquals(w.toString(), w.toString());
  }
}
