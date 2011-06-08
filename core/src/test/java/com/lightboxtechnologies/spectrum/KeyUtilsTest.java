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

import org.apache.commons.codec.binary.Hex;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Joel Uckelman
 */
public class KeyUtilsTest {
 @Test
  public void makeOutKeyMD5() throws Exception {
    final Hex hex = new Hex(); 
    final byte[] hash =
      hex.decode("8a9111fe05f9815fc55c728137c5b389".getBytes());
    final byte type = 0;
    final byte[] col = "vassalengine.org/VASSAL 3.1.15".getBytes();

    // It's neat that I have to cast one-byte numeric literals to bytes.
    // Thanks, Java!
    final byte[] expected = {
      (byte) 0x8a, (byte) 0x91, (byte) 0x11, (byte) 0xfe, // MD5
      (byte) 0x05, (byte) 0xf9, (byte) 0x81, (byte) 0x5f,
      (byte) 0xc5, (byte) 0x5c, (byte) 0x72, (byte) 0x81,
      (byte) 0x37, (byte) 0xc5, (byte) 0xb3, (byte) 0x89,
      (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, // padding
      (byte) 0x00,                                        // type
      'v', 'a', 's', 's', 'a', 'l', 'e', 'n', 'g',        // column
      'i', 'n', 'e', '.', 'o', 'r', 'g', '/', 'V',
      'A', 'S', 'S', 'A', 'L', ' ', '3', '.', '1',
      '.', '1', '5'
    };

    assertArrayEquals(expected, KeyUtils.makeEntryKey(hash, type, col));
  }

  @Test
  public void makeOutKeySHA1() throws Exception {
    final Hex hex = new Hex(); 
    final byte[] hash =
      hex.decode("64fa477898e268fd30c2bfe272e5a016f5ec31c4".getBytes());
    final byte type = 1;
    final byte[] col = "vassalengine.org/VASSAL 3.1.15".getBytes();

    // It's neat that I have to cast one-byte numeric literals to bytes.
    // Thanks, Java!
    final byte[] expected = {
      (byte) 0x64, (byte) 0xfa, (byte) 0x47, (byte) 0x78, // SHA1
      (byte) 0x98, (byte) 0xe2, (byte) 0x68, (byte) 0xfd,
      (byte) 0x30, (byte) 0xc2, (byte) 0xbf, (byte) 0xe2,
      (byte) 0x72, (byte) 0xe5, (byte) 0xa0, (byte) 0x16,
      (byte) 0xf5, (byte) 0xec, (byte) 0x31, (byte) 0xc4,
      (byte) 0x01,                                        // type
      'v', 'a', 's', 's', 'a', 'l', 'e', 'n', 'g',        // column
      'i', 'n', 'e', '.', 'o', 'r', 'g', '/', 'V',
      'A', 'S', 'S', 'A', 'L', ' ', '3', '.', '1',
      '.', '1', '5'
    };

    assertArrayEquals(expected, KeyUtils.makeEntryKey(hash, type, col));
  }
}
