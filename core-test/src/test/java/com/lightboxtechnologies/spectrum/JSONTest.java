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

import org.json.simple.JSONObject;

/**
 * @author Joel Uckelman
 */
public class JSONTest {
  @SuppressWarnings("unchecked")
  @Test(expected=JSON.DataException.class)
  public void testGetAsNullKey() throws JSON.DataException {
    final JSONObject json = new JSONObject();
    JSON.getAs(json, null, String.class);
  }

  @SuppressWarnings("unchecked")
  @Test(expected=JSON.DataException.class)
  public void testGetAsNullValue() throws JSON.DataException {
    final JSONObject json = new JSONObject();
    final String key = "foo";
    json.put(key, null);
    JSON.getAs(json, key, String.class);
  }

  @SuppressWarnings("unchecked")
  @Test(expected=JSON.DataException.class)
  public void testGetAsMissingKey() throws JSON.DataException {
    final JSONObject json = new JSONObject();
    final String key = "foo";
    JSON.getAs(json, key, String.class);
  }

  @SuppressWarnings("unchecked")
  @Test(expected=JSON.DataException.class)
  public void testGetAsTypeMismatch() throws JSON.DataException {
    final JSONObject json = new JSONObject();
    final String key = "foo";
    json.put(key, 42);
    JSON.getAs(json, key, JSONObject.class);
  }

  @SuppressWarnings("unchecked")
  public void testGetAsOk() throws JSON.DataException {
    final JSONObject json = new JSONObject();
    final String key = "foo";
    final Integer val = 42;
    json.put(key, val);
    assertEquals(val, JSON.getAs(json, key, Integer.class));
  }

  @SuppressWarnings("unchecked")
  public void testGetNonNullOk() throws JSON.DataException {
    final JSONObject json = new JSONObject();
    final String key = "foo";
    json.put(key, 42);
    assertTrue(JSON.getNonNull(json, key) != null);
  }

  @SuppressWarnings("unchecked")
  @Test(expected=JSON.DataException.class)
  public void testGetNonNullNullKey() throws JSON.DataException {
    final JSONObject json = new JSONObject();
    JSON.getNonNull(json, null);
  }

  @SuppressWarnings("unchecked")
  @Test(expected=JSON.DataException.class)
  public void testGetNonNullNullValue() throws JSON.DataException {
    final JSONObject json = new JSONObject();
    final String key = "foo";
    json.put(key, null);
    JSON.getNonNull(json, key);
  }

  @SuppressWarnings("unchecked")
  @Test(expected=JSON.DataException.class)
  public void testGetNonNullMissingKey() throws JSON.DataException {
    final JSONObject json = new JSONObject();
    final String key = "foo";
    JSON.getNonNull(json, key);
  }
}
