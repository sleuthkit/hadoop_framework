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
