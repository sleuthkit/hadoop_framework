package com.lightboxtechnologies.nsrl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import static com.lightboxtechnologies.nsrl.HashLoader.HashLoaderMapper;

/**
 * A helper class for writing NSRL data to {@link HFileOutputFormat}.
 *
 * @author Joel Uckelman
 */
class HashLoaderHelper {

  public HashLoaderHelper(Map<Integer,List<ProdData>> prod,
                          Map<String,MfgData> mfg,
                          Map<String,OSData> os, long timestamp) {
    this.prod = prod;
    this.mfg = mfg;
    this.timestamp = timestamp;
  } 

  private final Map<Integer,List<ProdData>> prod; 
  private final Map<String,MfgData> mfg;

  private final long timestamp;

  private final byte[] size = new byte[Bytes.SIZEOF_LONG];
  private final ImmutableBytesWritable okey = new ImmutableBytesWritable();

  private static final byte[] family = { '0' };

  // common column names
  private static final byte[] sha1_col = "sha1".getBytes();
  private static final byte[] md5_col = "md5".getBytes();
  private static final byte[] crc32_col = "crc32".getBytes();
  private static final byte[] size_col = "filesize".getBytes();
  private static final byte[] nsrl_col = "NSRL".getBytes();

  private static final byte[] one = { 1 };

  public static byte[] makeOutKey(byte[] hash, byte type, byte[] colname) {
    /*
      Key format is:

        hash right-padded with zeros to 20 bytes
        byte 0 for md5, 1 for sha1
        column name as bytes

      The reason for padding the hash and including the type byte is to
      ensure that MD5s which are prefixes of SHA1s can be distinguished
      from them, yet still sort correctly.
    */

    final byte[] okbytes = new byte[20+1+colname.length];
    Bytes.putBytes(okbytes, 0, hash, 0, hash.length);
    okbytes[20] = type;
    Bytes.putBytes(okbytes, 21, colname, 0, colname.length);
    return okbytes;
  }

  public void writeRow(byte[] key, HashData hd,
                       HashLoaderMapper.Context context)
                                     throws InterruptedException, IOException {
    // md5 is type 0, sha1 is type 1
    final byte ktype = (byte) (key.length == 16 ? 0 : 1);

    // write the crc32 column
    okey.set(makeOutKey(key, ktype, crc32_col));
    context.write(
      okey, new KeyValue(key, family, crc32_col, timestamp, hd.crc32)
    );

    switch (ktype) {
    case 0:
      // write the sha1 column if the key is not the sha1
      okey.set(makeOutKey(key, ktype, sha1_col));
      context.write(
        okey, new KeyValue(key, family, sha1_col, timestamp, hd.sha1)
      );
      break;
    case 1:
      // write the md5 column if the key is not the md5
      okey.set(makeOutKey(key, ktype, md5_col));
      context.write(
        okey, new KeyValue(key, family, md5_col, timestamp, hd.md5)
      );
      break;
    }

    // write the file size
    Bytes.putLong(size, 0, hd.size);
    okey.set(makeOutKey(key, ktype, size_col));
    context.write(
      okey, new KeyValue(key, family, size_col, timestamp, size)
    );

    // check the NSRL box
    okey.set(makeOutKey(key, ktype, nsrl_col));
    context.write(okey, new KeyValue(key, family, nsrl_col, timestamp, one));

    // look up the product data
    final List<ProdData> pl = prod.get(hd.prod_code);

    // check the manufacturer/product/os box for each product
    for (ProdData pd : pl) {
      final MfgData pmd = mfg.get(pd.mfg_code);

      final byte[] set_col =
        (pmd.name + '/' + pd.name + ' ' + pd.version).getBytes();

      okey.set(makeOutKey(key, ktype, set_col));
      context.write(okey, new KeyValue(key, family, set_col, timestamp, one));
    }
  }
}
