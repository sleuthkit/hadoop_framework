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
