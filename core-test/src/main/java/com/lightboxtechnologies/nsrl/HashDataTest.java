package com.lightboxtechnologies.nsrl;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * @author Joel Uckelman
 */
public class HashDataTest {
  private static final Hex hex = new Hex();

  private static final String name = "c'est ne pas un name";
  private static final long size = 42;
  private static final int prod_code = 13;
  private static final String os_code = "89";
  private static final String special_code = "so fucking special";

  private static byte[] sha1;
  private static byte[] md5;
  private static byte[] crc32;

  @BeforeClass
  public static void init() throws DecoderException {
    sha1  = (byte[]) hex.decode("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    md5   = (byte[]) hex.decode("deadbeefdeadbeefdeadbeefdeadbeef");
    crc32 = (byte[]) hex.decode("deadbeef");
  }

  @Test
  public void equalsSelf() {
    final HashData hd = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);

    assertTrue(hd.equals(hd)); 
  }

  @Test
  public void equalsSame() {
    final HashData a = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);
    final HashData b = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);

    assertTrue(a.equals(b)); 
    assertTrue(b.equals(a)); 
  }

  @Test
  public void notEqualsNull() {
    final HashData hd = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);

    assertFalse(hd.equals(null)); 
  }

  @Test
  public void notEqualsOtherType() {
    final HashData hd = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);

    assertFalse(hd.equals("bogus")); 
    assertFalse("bogus".equals(hd)); 
  }

  @Test
  public void notEqualsOtherDifferentProdCode() {
    final HashData a = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);
    final HashData b = new HashData(
      sha1, md5, crc32, name, size, prod_code+1, os_code, special_code);

    assertFalse(a.equals(b));
    assertFalse(b.equals(a));
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullSHA1() {
    new HashData(null, new byte[16], new byte[4], "", 0, 0, "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void badSHA1() {
    new HashData(new byte[0], new byte[16], new byte[4], "", 0, 0, "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullMD5() {
    new HashData(new byte[20], null, new byte[4], "", 0, 0, "", "");
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void badMD5() {
    new HashData(new byte[20], new byte[0], new byte[4], "", 0, 0, "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullCRC32() {
    new HashData(new byte[20], new byte[16], null, "", 0, 0, "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void badCRC32() {
    new HashData(new byte[20], new byte[16], new byte[0], "", 0, 0, "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullName() {
    new HashData(new byte[20], new byte[16], new byte[4], null, 0, 0, "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void negativeSize() {
    new HashData(new byte[20], new byte[16], new byte[4], "", -1, 0, "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void negativeProdCode() {
    new HashData(new byte[20], new byte[16], new byte[4], "", 0, -1, "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullOSCode() {
    new HashData(new byte[20], new byte[16], new byte[4], "", 0, 0, null, "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullSpecialCode() {
    new HashData(new byte[20], new byte[16], new byte[4], "", 0, 0, "", null);
  }

  @Test
  public void hashCodeSelf() {
    final HashData hd = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);

    assertTrue(hd.hashCode() == hd.hashCode()); 
  }

  @Test
  public void hashCodeSame() {
    final HashData a = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);
    final HashData b = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);

    assertTrue(a.hashCode() == b.hashCode()); 
  }

  @Test
  public void hashCodeDifferent() throws DecoderException {
    final byte[] sha1alt =
      (byte[]) hex.decode("0000000000000000000000000000000000000000");

    final HashData a = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);
    final HashData b = new HashData(
      sha1alt, md5, crc32, name, size, prod_code, os_code, special_code);

    assertTrue(a.hashCode() != b.hashCode());
  }
}
