
package com.lightboxtechnologies.spectrum;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Joel Uckelman
 */
public class KeyUtils {

  public static final byte MD5 = 0;
  public static final byte SHA1 = 1;

  public static final int MD5_LEN = 16;
  public static final int SHA1_LEN = 20;

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

  public static int getHashLength(byte[] key) {
    byte type = key[20];
    switch (type) {
      case MD5:
        return MD5_LEN;
      case SHA1:
        return SHA1_LEN;
      default:
        return 0;
    }
  }

  public static byte[] getHash(byte[] key) {
    byte[] hash = new byte[getHashLength(key)];
    getHash(hash, key);
    return hash;
  }

  public static int getHash(byte[] hash, byte[] key) {
    return Bytes.putBytes(hash, 0, key, 0, getHashLength(key));
  }

  public static byte[] getFsEntryID(byte[] key) {
    byte[] id = new byte[key.length - 21];
    getFsEntryID(id, key);
    return id;
  }

  public static int getFsEntryID(byte[] id, byte[] key) {
    return Bytes.putBytes(id, 0, key, 21, key.length - 21);
  }

  public static byte[] getImageID(byte[] key) {
    byte[] id = new byte[MD5_LEN];
    getImageID(id, key);
    return id;
  }

  public static int getImageID(byte[] imgID, byte[] key) {
    return FsEntryUtils.getImageID(imgID, key, 21);
  }

  public static boolean isType2(byte[] key) {
    return key.length > 20;
  }
}
