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

import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;

/**
 * An NSRL hash record.
 *
 * @author Joel Uckelman
 */
public class HashData {
  public final byte[] sha1;
  public final byte[] md5;
  public final byte[] crc32;
  public final String name;
  public final long size;
  public final int prod_code;
  public final String os_code;
  public final String special_code;

  public HashData(
    byte[] sha1, byte[] md5, byte[] crc32, String name, long size,
    int prod_code, String os_code, String special_code)
  {
    if (sha1 == null) throw new IllegalArgumentException();
    if (sha1.length != 20) throw new IllegalArgumentException();
    if (md5 == null) throw new IllegalArgumentException();
    if (md5.length != 16) throw new IllegalArgumentException();
    if (crc32 == null) throw new IllegalArgumentException();
    if (crc32.length != 4) throw new IllegalArgumentException();
    if (name == null) throw new IllegalArgumentException();
    if (size < 0) throw new IllegalArgumentException();
    if (prod_code < 0) throw new IllegalArgumentException();
    if (os_code == null) throw new IllegalArgumentException();
    if (special_code == null) throw new IllegalArgumentException();

    this.sha1 = sha1;
    this.md5 = md5;
    this.crc32 = crc32;
    this.name = name;
    this.size = size;
    this.prod_code = prod_code;
    this.os_code = os_code;
    this.special_code = special_code;
  }

  @Override
  public String toString() {
    return String.format(
      "%s[sha1=\"%s\",md5=\"%s\",crc32=\"%s\",name=\"%s\",size=%d,prod_code=%d,os_code=\"%s\",special_code=\"%s\"]",
      getClass().getName(), Hex.encodeHexString(sha1),
      Hex.encodeHexString(md5), Hex.encodeHexString(crc32),
      name, size, prod_code, os_code, special_code
    );
  }

    @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HashData)) return false;
    final HashData d = (HashData) o;
    return Arrays.equals(sha1, d.sha1) && Arrays.equals(md5, d.md5) &&
           Arrays.equals(crc32, d.crc32) && name.equals(d.name) &&
           size == d.size && prod_code == d.prod_code &&
           os_code.equals(d.os_code) && special_code.equals(d.special_code);
  }

  @Override
  public int hashCode() {
    return sha1.hashCode();
  }
}
