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

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.hbase.client.Put;

import com.lightboxtechnologies.spectrum.FsEntryHBaseOutputFormat.*;

public class FsEntryHBaseOutputFormatTest {
  @Test
  public void testWrite() {
    FsEntry entry = new FsEntry();
    entry.parseJson("{\"path\":\"a/path/\", \"meta\":{\"size\":35, \"crtime\":258970620, \"mtime\":285249600, \"atime\":1123948800, \"ctime\":1261555200}, \"name\":{\"name\":\"jon\",\"dirIndex\":2}}");
    Put p = FsEntryHBaseWriter.createPut(entry.fullPath(), entry, new byte[] {0x00, 0x01, 0x02, 0x03});
    assertEquals(1, p.numFamilies());
    assertEquals(entry.size(), p.size());
  }

  @Test
  public void testAttrWrite() {
    FsEntry entry = new FsEntry();
    entry.parseJson("{\"path\":\"a/path/\", \"meta\":{\"size\":35, \"crtime\":258970620, \"mtime\":285249600, \"atime\":1123948800, \"ctime\":1261555200}, \"name\":{\"name\":\"jon\",\"dirIndex\":2},"
      + "\"attrs\":[{\"flags\":5,\"id\":0,\"name\":\"N/A\",\"size\":72,\"type\":16,\"rd_buf_size\":1024,\"nrd_allocsize\":0,\"nrd_compsize\":0,\"nrd_initsize\":0,\"nrd_skiplen\":0, \"rd_buf\":\"d8dacd25d2e3c901d8dacd25d2e3c901d8dacd25d2e3c901d8dacd25d2e3c90100000000000000000000000000000000000000000102000000000000000000000000000000000000\"}, {\"flags\":5,\"id\":2,\"name\":\"N/A\",\"size\":74,\"type\":48,\"rd_buf_size\":1024,\"nrd_allocsize\":0,\"nrd_compsize\":0,\"nrd_initsize\":0,\"nrd_skiplen\":0, \"rd_buf\":\"4262000000000100d8dacd25d2e3c901d8dacd25d2e3c901d8dacd25d2e3c901d8dacd25d2e3c9010000000000000000000000000000000000000010000000000403490053004f007300\"}, {\"flags\":5,\"id\":1,\"name\":\"$I30\",\"size\":48,\"type\":144,\"rd_buf_size\":1024,\"nrd_allocsize\":0,\"nrd_compsize\":0,\"nrd_initsize\":0,\"nrd_skiplen\":0, \"rd_buf\":\"300000000100000000100000010000001000000020000000200000000000000000000000000000001000000002000000\"}]}");
    Put p = FsEntryHBaseWriter.createPut(entry.fullPath(), entry, new byte[] {0x00, 0x01, 0x02, 0x03});
    assertEquals(1, p.numFamilies());
    assertEquals(entry.size(), p.size());
  }
}
