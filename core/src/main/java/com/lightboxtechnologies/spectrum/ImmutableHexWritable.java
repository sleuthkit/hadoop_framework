/*
src/com/lightboxtechnologies/spectrum/ImmutableHexWritable.java

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

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * @author Joel Uckelman
 */
public class ImmutableHexWritable extends ImmutableBytesWritable {
  /** @inheritDoc */
  public ImmutableHexWritable() {
    super();
  }

  /** @inheritDoc */
  public ImmutableHexWritable(byte[] bytes) {
    super(bytes);
  }

  /** @inheritDoc */
  public ImmutableHexWritable(final ImmutableBytesWritable ibw) {
    super(ibw);
  }

  /** @inheritDoc */
  public ImmutableHexWritable(final byte[] bytes, final int offset, final int length) {
    super(bytes, offset, length);
  }

  @Override
  public String toString() {
    // NB: This follows the broken behavior of the superclass's toString(),
    // which also ignores the offset and length.
    final Hex hex = new Hex();
    final byte[] bytes = get();
    return new String(hex.encode(bytes));
  }
}
