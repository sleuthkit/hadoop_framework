package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Joel Uckelman
 */
public class ProdDataTest {
  private static final int code = 99;
  private static final String name = "Lern Yerself Scouse";
  private static final String version = "1.0";
  private static final String os_code = "42";
  private static final String mfg_code = "Lime Street Applications Inc.";
  private static final String language = "Scouse";
  private final String app_type = "Langauge learning";

  @Test
  public void equalsSelf() {
    final ProdData pd =
      new ProdData(code, name, version, os_code, mfg_code, language, app_type);
    assertTrue(pd.equals(pd)); 
  }

  @Test
  public void equalsSame() {
    final ProdData a =
      new ProdData(code, name, version, os_code, mfg_code, language, app_type);
    final ProdData b =
      new ProdData(code, name, version, os_code, mfg_code, language, app_type);

    assertTrue(a.equals(b)); 
    assertTrue(b.equals(a)); 
  }

  @Test
  public void notEqualsNull() {
    final ProdData pd =
      new ProdData(code, name, version, os_code, mfg_code, language, app_type);
    assertFalse(pd.equals(null)); 
  }

  @Test
  public void notEqualsOtherType() {
    final ProdData pd =
      new ProdData(code, name, version, os_code, mfg_code, language, app_type);

    assertFalse(pd.equals("bogus")); 
    assertFalse("bogus".equals(pd)); 
  }

  @Test
  public void notEqualsOtherDifferentMfgCode() {
    final ProdData a = new ProdData(
      code, name, version, os_code, mfg_code, language, app_type);
    final ProdData b = new ProdData(
      code, name, version, os_code, "gammy anded", language, app_type);

    assertFalse(a.equals(b));
    assertFalse(b.equals(a));
  }

  @Test(expected=IllegalArgumentException.class)
  public void negativeCode() {
    new ProdData(-1, "", "", "", "", "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullName() {
    new ProdData(1, null, "", "", "", "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullVersion() {
    new ProdData(1, "", null, "", "", "", "");
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void nullOSCode() {
    new ProdData(1, "", "", null, "", "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullMfgCode() {
    new ProdData(1, "", "", "", null, "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullLanguage() {
    new ProdData(1, "", "", "", "", null, "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullAppType() {
    new ProdData(1, "", "", "", "", "", null);
  }

  @Test
  public void hashCodeSelf() {
    final ProdData pd =
      new ProdData(code, name, version, os_code, mfg_code, language, app_type);

    assertTrue(pd.hashCode() == pd.hashCode()); 
  }

  @Test
  public void hashCodeSame() {
    final ProdData a =
      new ProdData(code, name, version, os_code, mfg_code, language, app_type);
    final ProdData b =
      new ProdData(code, name, version, os_code, mfg_code, language, app_type);
    assertTrue(a.hashCode() == b.hashCode()); 
  }

  @Test
  public void hashCodeDifferent() {
    final ProdData a = new ProdData(
      code, name, version, os_code, mfg_code, language, app_type);
    final ProdData b = new ProdData(
      code, name, version, os_code, "gammy anded", language, app_type);

    assertTrue(a.hashCode() != b.hashCode());
  }
}
