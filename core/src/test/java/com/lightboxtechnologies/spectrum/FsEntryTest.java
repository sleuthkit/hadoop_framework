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

import java.util.Date;
import java.io.InputStream;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class FsEntryTest {
  @Test
  public void testParseJson() {
    FsEntry e = new FsEntry();
    assertEquals(null, e.getPath());
    assertTrue(e.parseJson("{\"path\":\"/my/path/\", \"name\":{\"name\":\"myname\",\"flags\":3,\"meta_seq\":2,\"type\":2,\"dirIndex\":2}}"));
    assertEquals("/my/path/", e.getPath());
    assertEquals("myname", e.getName());
    assertEquals("/my/path/myname-2", e.getID());
    assertEquals(null, e.getCreated());
    assertEquals(3L, e.get("name_flags"));
    assertEquals(2L, e.get("meta_seq"));
    assertEquals(2L, e.get("name_type"));
    assertEquals(2L, e.get("dirIndex"));
  }

  @Test
  public void testNoName() {
    FsEntry e = new FsEntry();
    assertFalse(e.parseJson("{\"path\":12345, \"name\":\"whatever\"}"));
    assertEquals(null, e.getPath());
    assertEquals(null, e.getName());
  }

  @Test
  public void testNoDirIndex() {
    FsEntry e = new FsEntry();
    assertFalse(e.parseJson("{\"path\":12345, \"name\":{\"name\":\"aname\"}}"));
    assertEquals(null, e.getPath());
    assertEquals(null, e.getName());
  }

  @Test
  public void testMetadataParse() { // 3/17/1978 08:17:00
    FsEntry e = new FsEntry();
    assertEquals(null, e.getCreated());
    assertEquals(null, e.getWritten());
    assertEquals(null, e.getAccessed());
    assertEquals(null, e.getUpdated());
    assertEquals(0, e.getSize());
    assertTrue(e.parseJson("{\"path\":\"a/path/\", \"meta\":{\"size\":35, \"crtime\":258970620, \"mtime\":285249600, \"atime\":1123948800, \"ctime\":1261555200}, \"name\":{\"name\":\"jon\",\"dirIndex\":2}}"));
    Date d = e.getCreated();
    assertEquals(258970620000L, d.getTime());
    d = e.getWritten();
    assertEquals(285249600000L, d.getTime());
    d = e.getAccessed();
    assertEquals(1123948800000L, d.getTime());
    d = e.getUpdated();
    assertEquals(1261555200000L, d.getTime());
    assertEquals(35, e.getSize());
  }

  @Test
  public void testNoMetaRecord() {
    FsEntry e = new FsEntry();
    assertFalse(e.hasMetadata());
    assertTrue(e.parseJson("{\"path\":\"a/path/\", \"name\":{\"name\":\"aname\",\"dirIndex\":2}}"));
    assertFalse(e.hasMetadata());
    assertTrue(e.parseJson("{\"path\":\"a/path/\", \"name\":{\"name\":\"aname\",\"dirIndex\":2}, \"meta\":{\"size\":1, \"crtime\":258970620, \"mtime\":285249600, \"atime\":1123948800, \"ctime\":1261555200}}"));
    assertTrue(e.hasMetadata());
  }

  @Test
  public void testFullPath() {
    FsEntry e = new FsEntry();
    assertTrue(e.parseJson("{\"path\":\"a/path/\", \"name\":{\"name\":\"aname\",\"dirIndex\":2}}"));
    assertEquals("a/path/aname", e.fullPath());
    assertTrue(e.parseJson("{\"path\":\"\", \"name\":{\"name\":\"small\",\"dirIndex\":2}}"));
    assertEquals("small", e.fullPath());
  }

  @Test
  public void testMap() {
    FsEntry e = new FsEntry();
    assertTrue(e.parseJson("{\"path\":\"a/path/\", \"name\":{\"name\":\"aname\"}, \"meta\":{\"size\":1, \"crtime\":258970620, \"mtime\":285249600, \"atime\":1123948800, \"ctime\":1261555200}}"));
    assertEquals("a/path/", e.get("path"));
    assertEquals("aname", e.get("name"));
    assertEquals(1L, ((Long)e.get("size")).longValue());
    assertEquals(258970620000L, ((Date)e.get("created")).getTime());
    assertEquals(285249600000L, ((Date)e.get("written")).getTime());
    assertEquals(1123948800000L, ((Date)e.get("accessed")).getTime());
    assertEquals(1261555200000L, ((Date)e.get("updated")).getTime());
  }

  @Test
  public void testOtherMeta() {
    FsEntry e = new FsEntry();
    assertTrue(e.parseJson("{\"path\":\"a/path/\", \"name\":{\"name\":\"aname\",\"dirIndex\":2}, \"meta\":{\"size\":1, \"crtime\":258970620, \"mtime\":285249600, \"atime\":1123948800, \"ctime\":1261555200"
      + ", \"flags\":5,\"uid\":0,\"gid\":519,\"type\":2,\"seq\":3,\"mode\":511,\"content_len\":0,\"addr\":12345,\"nlink\":2}}"));
    assertEquals(5L, e.get("meta_flags"));
    assertEquals(0L, e.get("uid"));
    assertEquals(519L, e.get("gid"));
    assertEquals(2L, e.get("meta_type"));
    assertEquals(3L, e.get("seq"));
    assertEquals(511L, e.get("mode"));
    assertEquals(0L, e.get("content_len"));
    assertEquals(12345L, e.get("meta_addr"));
    assertEquals(2L, e.get("nlink"));
  }

  @Test
  public void testFsFields() {
    FsEntry e = new FsEntry();
    assertTrue(e.parseJson("{\"fs\":{\"byteOffset\":987,\"fsID\":\"12asdb4qw4\"},\"path\":\"a/path/\", \"name\":{\"name\":\"aname\",\"dirIndex\":2}}"));
    assertEquals(987L, e.get("fs_byte_offset"));
    assertEquals("12asdb4qw4", e.get("fs_id"));
  }

  @Test
  public void testAttrs() {
    FsEntry e = new FsEntry();
    JSONParser p = new JSONParser();
    Object obj = null;
    try {
      obj = p.parse("[{\"whatever\":1}, {\"yo\":\"mama\"}]");
    }
    catch (ParseException ex) {
      assertTrue(false);
    }
    assertTrue(e.parseJson("{\"path\":\"a/path/\", \"name\":{\"name\":\"aname\",\"dirIndex\":2}, \"attrs\":[{\"whatever\":1}, {\"yo\":\"mama\"}]}"));
    assertEquals(obj, e.get("attrs"));
    // map.put("attrs", rec.get("attrs")) -- just copy over the map directly--should be an array
  }

  @Test
  public void testAttrsBad() {
    FsEntry e = new FsEntry();
    assertFalse(e.parseJson("{\"path\":\"a/path/\", \"name\":{\"name:\":\"aname\",\"dirIndex\":2}, \"attrs\":[{\"whatever\":1}, {\"yo\":\"mama\"}]}"));
  }

  @Test
  public void testClear() {
    FsEntry e = new FsEntry();
    e.put("name", "foo");
    e.put("path", "bar");
    assertEquals("foo", e.getName());
    assertEquals("bar", e.getPath());
    e.clear();
    assertEquals(null, e.getName());
    assertEquals(null, e.getPath());
  }

  @Test
  public void testExtension() {
    FsEntry e = new FsEntry();
    e.put("name", "foo");
    assertEquals("", e.extension());

    e.clear();
    e.put("name", "foo.txt");
    assertEquals("txt", e.extension());

    e.clear();
    e.put("name", "foo..txt");
    assertEquals("txt", e.extension());

    e.clear();
    e.put("name", "foo.tar.gz");
    assertEquals("gz", e.extension());

    e.clear();
    e.put("name", "foo.html_file");
    assertEquals("html_file", e.extension());

    e.clear();
    e.put("name", "");
    assertEquals("", e.extension());

    e.clear();
    e.put("name", "foo.Html");
    assertEquals("html", e.extension());

    e.clear();
    e.put("name", "foo.");
    assertEquals("", e.extension());
  }

  @Test
  public void testGetInputStream() throws Exception {
    FsEntry e = new FsEntry();
    byte[] expected = {0, 1, 2, 3, 4, 5};
    byte[] actual   = new byte[6];
    
    e.getStreams().put("Content", new BufferProxy(expected));
    InputStream stream = e.getInputStream();
    assertTrue(stream != null);
    assertEquals(6, stream.read(actual)); // a bit dodgy relying on a full read, but it works
    assertEquals(-1, stream.read());
  }
}
