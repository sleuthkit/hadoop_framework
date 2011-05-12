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
