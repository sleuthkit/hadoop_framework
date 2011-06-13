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

// based on BitComparator in latest HBase

package com.lightboxtechnologies.spectrum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.hbase.filter.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A bit comparator which performs the specified bitwise operation on each of the bytes
 * with the specified byte array. Then returns whether the result is non-zero.
 */
public class FsEntryRowFilter extends WritableByteArrayComparable {
  private final Log LOG = LogFactory.getLog(FsEntryRowFilter.class);

  /** Nullary constructor for Writable, do not use */
  public FsEntryRowFilter() {}

  /**
   * Constructor
   * @param value value
   * @param BitwiseOp bitOperator - the operator to use on the bit comparison
   */
  public FsEntryRowFilter(byte[] value) {
    super(value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    LOG.info("Read in image ID " + new String(Hex.encodeHex(getValue())));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
  }

  @Override
  public int compareTo(byte[] value) {
    int ret = 0;
    byte[] key = getValue();
    if (value.length > key.length) {
      for (int i = 0; i < key.length; ++i) {
        if (key[i] != value[i + 1]) { // ignores first byte of value
          ret = 1;
          break;
        }
      }
    }
    else {
      ret = 1;
    }
    return ret;
  }
}
