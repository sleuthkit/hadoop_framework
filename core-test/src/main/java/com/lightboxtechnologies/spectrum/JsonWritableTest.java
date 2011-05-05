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
    final List<Long> input = Arrays.asList(new Long[] {5L, 89L, 2458L, 24L, 64L, 17L});
    assertEquals(6, input.size());
    final List<Long> expected = new ArrayList<Long>();
    expected.addAll(input);
    assertEquals(expected, input);

    final JsonWritable w1 = new JsonWritable();
    w1.set(input);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    w1.write(new DataOutputStream(out));

    final JsonWritable w2 = new JsonWritable();
    w2.readFields(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertArrayEquals(expected.toArray(), ((List)w2.get()).toArray());
  }

  @Test
  public void testSetMap() throws IOException {
    final Map<String,Object> input = new HashMap<String,Object>();
    final Map<String,Object> expected = new HashMap<String,Object>();

    input.put("three", 3L);
    input.put("hello", "world");
    input.put("empty", new HashMap<Object,Object>());
    expected.putAll(input);
    assertTrue(expected.equals(input));

    final JsonWritable w1 = new JsonWritable();
    w1.set(input);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    w1.write(new DataOutputStream(out));
    
    final JsonWritable w2 = new JsonWritable();
    w2.readFields(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertTrue(expected.equals(w2.get()));
  }
}
