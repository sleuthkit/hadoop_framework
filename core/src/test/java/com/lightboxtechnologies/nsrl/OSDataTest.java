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

package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Joel Uckelman
 */
public class OSDataTest {
  private static final String code = "YourMomsCode";
  private static final String name = "MomOS";
  private static final String version = "1.0";
  private static final String mfg_code = "YourMom";

  @Test
  public void equalsSelf() {
    final OSData osd = new OSData(code, name, version, mfg_code);
    assertTrue(osd.equals(osd));
  }

  @Test
  public void equalsSame() {
    final OSData a = new OSData(code, name, version, mfg_code);
    final OSData b = new OSData(code, name, version, mfg_code);
    assertTrue(a.equals(b));
    assertTrue(b.equals(a));
  }

  @Test
  public void notEqualsNull() {
    final OSData osd = new OSData(code, name, version, mfg_code);
    assertFalse(osd.equals(null));
  }

  @Test
  public void notEqualsOtherType() {
    final OSData osd = new OSData(code, name, version, mfg_code);
    assertFalse(osd.equals("bogus"));
    assertFalse("bogus".equals(osd));
  }

  @Test
  public void notEqualsOtherDifferentMfgCode() {
    final OSData a = new OSData(code, name, version, mfg_code);
    final OSData b = new OSData(code, name, version, "JonsMom");
    assertFalse(a.equals(b));
    assertFalse(b.equals(a));
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullCode() {
    new OSData(null, "", "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullName() {
    new OSData("", null, "", "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullVersion() {
    new OSData("", "", null, "");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullMfgCode() {
    new OSData("", "", "", null);
  }

  @Test
  public void hashCodeSelf() {
    final OSData osd = new OSData(code, name, version, mfg_code);
    assertTrue(osd.hashCode() == osd.hashCode());
  }

  @Test
  public void hashCodeSame() {
    final OSData a = new OSData(code, name, version, mfg_code);
    final OSData b = new OSData(code, name, version, mfg_code);
    assertTrue(a.hashCode() == b.hashCode());
  }

  @Test
  public void hashCodeDifferent() {
    final OSData a = new OSData(code, name, version, mfg_code);
    final OSData b = new OSData(code, name, version, "JonsMom");
    assertTrue(a.hashCode() != b.hashCode());
  }
}
