/*
src/com/lightboxtechnologies/spectrum/HexWritable.java

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

import org.apache.hadoop.io.BytesWritable;

/**
 * @author Joel Uckelman
 */
public class HexWritable extends BytesWritable {
  /** @inheritDoc */
  public HexWritable() {
    super();
  }

  /** @inheritDoc */
  public HexWritable(byte[] bytes) {
    super(bytes);
  }

  @Override
  public String toString() {
    final Hex hex = new Hex();
    final byte[] bytes = new byte[getLength()];
    System.arraycopy(getBytes(), 0, bytes, 0, bytes.length);
    return new String(hex.encode(bytes));
  }
}
