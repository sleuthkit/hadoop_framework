package com.lightboxtechnologies.nsrl;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * A {@link RecordProcessor} for NSRL hash records.
 *
 * @author Joel Uckelman
 */
class HashRecordProcessor implements RecordProcessor<HashData> {
  private static final Hex hex = new Hex();

  public HashData process(String[] col) throws BadDataException {
    if (col.length < 8) {
      throw new BadDataException("too few columns");
    }
    else if (col.length > 8) {
      throw new BadDataException("too many columns");
    }

    long size = 0;
    try {
      size = Long.parseLong(col[4]);
    }
    catch (NumberFormatException e) {
      throw new BadDataException(e);
    }

    if (size < 0) {
      throw new BadDataException("size < 0");
    }

    int prod_code = 0;
    try {
      prod_code = Integer.parseInt(col[5]);
    }
    catch (NumberFormatException e) {
      throw new BadDataException(e);
    }

    if (prod_code < 0) {
      throw new BadDataException("prod_code < 0");
    }

    byte[] sha1, md5, crc32;
    try {
      sha1  = (byte[]) hex.decode(col[0]);
      md5   = (byte[]) hex.decode(col[1]);
      crc32 = (byte[]) hex.decode(col[2]);
    }
    catch (DecoderException e) {
      throw new BadDataException(e);
    }

    return new HashData(
      sha1, md5, crc32, col[3], size, prod_code, col[6], col[7]
    );
  }
}
