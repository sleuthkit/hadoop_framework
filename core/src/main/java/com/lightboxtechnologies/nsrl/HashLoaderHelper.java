package com.lightboxtechnologies.nsrl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.lightboxtechnologies.spectrum.HBaseTables;
import com.lightboxtechnologies.spectrum.KeyUtils;

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

  // common column names
  private static final byte[] sha1_col = "sha1".getBytes();
  private static final byte[] md5_col = "md5".getBytes();
  private static final byte[] crc32_col = "crc32".getBytes();
  private static final byte[] size_col = "filesize".getBytes();
  private static final byte[] nsrl_col = "NSRL".getBytes();

  private static final byte[] empty = new byte[0];

  public void writeRow(byte[] key, HashData hd,
                       HashLoaderMapper.Context context)
                                     throws InterruptedException, IOException {
    final byte[] family = HBaseTables.HASH_COLFAM_B;

    // md5 is type 0, sha1 is type 1
    final byte ktype = (byte) (key.length == 16 ? 0 : 1);

    // write the crc32 column
    okey.set(KeyUtils.makeEntryKey(key, ktype, crc32_col));
    context.write(
      okey, new KeyValue(key, family, crc32_col, timestamp, hd.crc32)
    );

    switch (ktype) {
    case 0:
      // write the sha1 column if the key is not the sha1
      okey.set(KeyUtils.makeEntryKey(key, ktype, sha1_col));
      context.write(
        okey, new KeyValue(key, family, sha1_col, timestamp, hd.sha1)
      );
      break;
    case 1:
      // write the md5 column if the key is not the md5
      okey.set(KeyUtils.makeEntryKey(key, ktype, md5_col));
      context.write(
        okey, new KeyValue(key, family, md5_col, timestamp, hd.md5)
      );
      break;
    }

    // write the file size
    Bytes.putLong(size, 0, hd.size);
    okey.set(KeyUtils.makeEntryKey(key, ktype, size_col));
    context.write(okey, new KeyValue(key, family, size_col, timestamp, size));

    // check the NSRL box
    okey.set(KeyUtils.makeEntryKey(key, ktype, nsrl_col));
    context.write(okey, new KeyValue(key, family, nsrl_col, timestamp, empty));

    // look up the product data
    final List<ProdData> pl = prod.get(hd.prod_code);

    // check the manufacturer/product/os box for each product
    for (ProdData pd : pl) {
      final MfgData pmd = mfg.get(pd.mfg_code);

      final byte[] set_col =
        (pmd.name + '/' + pd.name + ' ' + pd.version).getBytes();

      okey.set(KeyUtils.makeEntryKey(key, ktype, set_col));
      context.write(okey, new KeyValue(key, family, set_col, timestamp, empty));
    }
  }
}
