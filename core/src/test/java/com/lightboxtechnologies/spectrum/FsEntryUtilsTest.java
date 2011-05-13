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

import static com.lightboxtechnologies.spectrum.FsEntryUtils.*;

/**
 * @author Joel Uckelman
 */
public class FsEntryUtilsTest {

  @Test
  public void makeFsEntryKeyTest() {
    final byte[] path = "goats/goats/goats/goat.jpg".getBytes();

    final int dir_index = 123456;

    final byte[] img_md5 = {
      (byte) 0xa0, (byte) 0x79, (byte) 0xc4, (byte) 0xe7,
      (byte) 0x79, (byte) 0xcd, (byte) 0x99, (byte) 0x1f,
      (byte) 0xbb, (byte) 0x8c, (byte) 0xe6, (byte) 0xfa,
      (byte) 0x49, (byte) 0x47, (byte) 0xdc, (byte) 0x2a
    };

    final byte[] expected = {
      (byte) 0x31,                                        // path_md5[0]
      (byte) 0xa0, (byte) 0x79, (byte) 0xc4, (byte) 0xe7, // img_md5  
      (byte) 0x79, (byte) 0xcd, (byte) 0x99, (byte) 0x1f,
      (byte) 0xbb, (byte) 0x8c, (byte) 0xe6, (byte) 0xfa,
      (byte) 0x49, (byte) 0x47, (byte) 0xdc, (byte) 0x2a,
                   (byte) 0x23, (byte) 0x4b, (byte) 0xf4, // rest of path_md5
      (byte) 0x10, (byte) 0x57, (byte) 0x58, (byte) 0xb5,
      (byte) 0x88, (byte) 0x07, (byte) 0x22, (byte) 0x5c,
      (byte) 0x26, (byte) 0x15, (byte) 0xc5, (byte) 0x72,
      (byte) 0x00, (byte) 0x01, (byte) 0xe2, (byte) 0x40  // dir_index
    };

    assertArrayEquals(expected, makeFsEntryKey(img_md5, path, dir_index));
  }
}
