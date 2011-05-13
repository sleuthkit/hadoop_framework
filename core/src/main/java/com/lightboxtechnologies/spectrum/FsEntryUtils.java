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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;


/**
 * @author Joel Uckelman
 */
public class FsEntryUtils {

  protected FsEntryUtils() {}

  public static byte[] makeFsEntryKey(byte[] img_md5, byte[] path, int dir_index) {
    /*
      FsEntry key is:

      bytes
      ----------------------------------------------------
          0  first byte of MD5 hash of parent path
       1-16  image id MD5
      17-31  remaining 15 bytes of MD5 hash of parent path
      32-35  directory index

      The path MD5 is split in order to better spread the keys for one
      path over the keyspace.
    */

    if (img_md5.length != 16) {
      throw new IllegalArgumentException("Image hash is not an MD5 hash.");
    }

    if (dir_index < 0) {
      throw new IllegalArgumentException(
        "Directory index " + dir_index " < 0!"
      );
    }

    // find the last path separator
    int lastsep = -1;
    for (int i = path.length-1; i >= 0; --i) {
      if (path[i] == '/') {
        lastsep = i;
        break;
      }
    }
  
    if (lastsep == -1) {
      throw new RuntimeException(
        "Path \"" + (new String(path)) + "\" contains no path separator!"
      );
    }
 
    // hash the base file path
    MessageDigest hasher = null;
    try {
      hasher = MessageDigest.getInstance("MD5");
    }
    catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException(
        "As if MD5 isn't going to be implemented, bloody Java tossers"
      );
    }

    hasher.update(path, 0, lastsep);
    final byte[] path_md5 = hasher.digest();

    // build the key
    final byte[] key = new byte[36]; // 2 md5 + int
    key[0] =  path_md5[0];
    int off = 1;
    off = Bytes.putBytes(key, off, img_md5, 0, img_md5.length);
    off = Bytes.putBytes(key, off, path_md5, 1, path_md5.length-1);
    Bytes.putInt(key, off, dir_index);

    return key;
  }
}
