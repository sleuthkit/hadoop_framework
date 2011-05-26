/*
test/com/lightboxtechnologies/spectrum/HexWritableTest.java

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

import org.apache.commons.codec.binary.Hex;

/**
 * @author Joel Uckelman
 */
public class HexWritableTest {
  @Test
  public void toStringTest() throws Exception {
    final Hex hex = new Hex();
    final String str = "DEADBEEF";
    final HexWritable hw = new HexWritable(hex.decode(str.getBytes()));
    assertEquals(str, hw.toString().toUpperCase());
  }
}
