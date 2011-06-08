
package com.lightboxtechnologies.spectrum;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Joel Uckelman
 */
public class KeyUtils {

  public static byte[] makeEntryKey(byte[] hash, byte type, byte[] data) {
    /*
      Key format is:

        hash right-padded with zeros to 20 bytes
        byte 0 for md5, 1 for sha1
        data as bytes

      The reason for padding the hash and including the type byte is to
      ensure that MD5s which are prefixes of SHA1s can be distinguished
      from them, yet still sort correctly.
    */

    final byte[] okbytes = new byte[20+1+data.length];
    Bytes.putBytes(okbytes, 0, hash, 0, hash.length);
    okbytes[20] = type;
    Bytes.putBytes(okbytes, 21, data, 0, data.length);
    return okbytes;
  }

}
