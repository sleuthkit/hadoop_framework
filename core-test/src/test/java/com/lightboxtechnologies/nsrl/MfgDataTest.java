package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Joel Uckelman
 */
public class MfgDataTest {
  private static final String code = "SPFT";
  private static final String name = "Stay Puft Marshmallow Corporation";

  @Test
  public void equalsSelf() {
    final MfgData osd = new MfgData(code, name);
    assertTrue(osd.equals(osd));
  }

  @Test
  public void equalsSame() {
    final MfgData a = new MfgData(code, name);
    final MfgData b = new MfgData(code, name);
    assertTrue(a.equals(b));
    assertTrue(b.equals(a));
  }

  @Test
  public void notEqualsNull() {
    final MfgData osd = new MfgData(code, name);
    assertFalse(osd.equals(null));
  }

  @Test
  public void notEqualsOtherType() {
    final MfgData osd = new MfgData(code, name);
    assertFalse(osd.equals("bogus"));
    assertFalse("bogus".equals(osd));
  }

  @Test
  public void notEqualsOtherDifferentCode() {
    final MfgData a = new MfgData(code, name);
    final MfgData b = new MfgData("FOOF", name);
    assertFalse(a.equals(b));
    assertFalse(b.equals(a));
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullCode() {
    new MfgData(null, "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullName() {
    new MfgData("", null);
  }

  @Test
  public void hashCodeSelf() {
    final MfgData osd = new MfgData(code, name);
    assertTrue(osd.hashCode() == osd.hashCode());
  }

  @Test
  public void hashCodeSame() {
    final MfgData a = new MfgData(code, name);
    final MfgData b = new MfgData(code, name);
    assertTrue(a.hashCode() == b.hashCode());
  }

  @Test
  public void hashCodeDifferent() {
    final MfgData a = new MfgData(code, name);
    final MfgData b = new MfgData("FOOF", name);
    assertTrue(a.hashCode() != b.hashCode());
  }
}
