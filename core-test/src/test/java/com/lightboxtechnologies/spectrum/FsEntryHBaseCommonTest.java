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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.util.Bytes;

import org.json.simple.JSONObject;

import com.lightboxtechnologies.spectrum.FsEntryHBaseCommon.*;
import static com.lightboxtechnologies.spectrum.FsEntryHBaseCommon.*;

public class FsEntryHBaseCommonTest {
  @Test
  public void testLong() {
    assertEquals(LONG, typeVal(7L));
  }

  @Test
  public void testString() {
    assertEquals(STRING, typeVal("string"));
  }

  @Test
  public void testDate() {
    assertEquals(DATE, typeVal(new Date()));
  }

  @Test
  public void testJson() {
    assertEquals(JSON, typeVal(new JSONObject()));
  }

  @Test
  public void testByteArray() {
    assertEquals(BYTE_ARRAY, typeVal(new byte[] {0x01, 0x02, 0x03}));
  }

  @Test
  public void testStream() {
    assertEquals(BUFFER_STREAM, typeVal(new BufferProxy(new byte[] {0x01, 0x02, 0x03, 0x04})));
  }

  @Test
  public void testFileStream() {
    assertEquals(FILE_STREAM, typeVal(new FileProxy("a/bogus/path.txt")));
  }

  @Test
  public void testCreateColSpec() {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    stream.write(0);
    stream.write(65);
    byte[] expected = stream.toByteArray();
    byte[] actual = createColSpec(7L, "A");
    assertEquals(expected.length, actual.length);
    assertEquals(2, actual.length);
    assertEquals(expected[0], actual[0]);
    assertEquals(expected[1], actual[1]);
  }

  @Test
  public void testColName() {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    stream.write(0);
    stream.write(65);
    assertEquals("A", colName(stream.toByteArray()));
  }

  @Test
  public void testUnmarshallLong() {
    ByteArrayOutputStream colSpec = new ByteArrayOutputStream();
    colSpec.write(LONG);
    colSpec.write(65);
    byte[] c = colSpec.toByteArray();
    assertEquals(7L, unmarshall(c, Bytes.toBytes(7L)));
  }

  @Test
  public void testUnmarshallString() {
    ByteArrayOutputStream colSpec = new ByteArrayOutputStream();
    colSpec.write(STRING);
    colSpec.write(65);
    byte[] c = colSpec.toByteArray();
    assertEquals("Test", unmarshall(c, Bytes.toBytes("Test")));
  }

  @Test
  public void testUnmarshallDate() {
    ByteArrayOutputStream colSpec = new ByteArrayOutputStream();
    colSpec.write(DATE);
    colSpec.write(65);
    byte[] c = colSpec.toByteArray();
    Date d = new Date(1267313232000L);
    assertEquals(d, unmarshall(c, Bytes.toBytes(d.getTime())));
  }

  @Test
  public void testUnmarshallJson() {
    ByteArrayOutputStream colSpec = new ByteArrayOutputStream();
    colSpec.write(JSON);
    colSpec.write('{');
    colSpec.write('}');
    byte[] c = colSpec.toByteArray();
    Map<String,Object> m = new HashMap<String,Object>();
    assertEquals(m, unmarshall(c, Bytes.toBytes("{}")));
  }

  @Test
  public void testUnmarshallByteArray() {
    ByteArrayOutputStream colSpec = new ByteArrayOutputStream();
    colSpec.write(BYTE_ARRAY);
    colSpec.write('D');
    byte[] c = colSpec.toByteArray();
    byte[] input = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05},
           expected = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05};
    assertArrayEquals(expected, (byte[])unmarshall(c, input));
  }

  @Test
  public void testUnmarshallBufferStream() throws IOException {
    ByteArrayOutputStream colSpec = new ByteArrayOutputStream();
    colSpec.write(BUFFER_STREAM);
    colSpec.write('s');
    byte[] c = colSpec.toByteArray(),
           input = new byte[] {0x01, 0x02, 0x03, 0x04},
           expected = new byte[4];
    StreamProxy proxy = (StreamProxy)unmarshall(c, input);
    InputStream stream = proxy.open(new RawLocalFileSystem());
    assertEquals(4, stream.read(expected));
    assertArrayEquals(expected, input);
    assertEquals(0, stream.available());
  }

  @Test
  public void testUnmarshallFileStream() {
    ByteArrayOutputStream colSpec = new ByteArrayOutputStream();
    colSpec.write(FILE_STREAM);
    colSpec.write('f');
    String path = "a/path/to/a/file.txt";
    byte[] c = colSpec.toByteArray(),
           input = Bytes.toBytes(path);
    StreamProxy proxy = (StreamProxy)unmarshall(c, input);
    FileProxy fp = (FileProxy)proxy;
    assertEquals(path, fp.getPath());
  }

  @Test
  public void testPopulate() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Map<byte[], byte[]> inMap = new HashMap<byte[], byte[]>();
    Map<String,Object> actualMap = new HashMap<String,Object>(),
                     expectedMap = new HashMap<String,Object>();

    Map<String, StreamProxy> actualStreams = new HashMap<String, StreamProxy>(),
                           expectedStreams = new HashMap<String, StreamProxy>();

    // what about JSON?

    Date ts = new Date();

    expectedMap.put("num", 17L);
    expectedMap.put("whatevs", "a string");
    expectedMap.put("exfiltrated", ts.clone());

    expectedStreams.put("Content", new FileProxy("some/bullshit/file.dat"));
    expectedStreams.put("Slack", new BufferProxy(new byte[] {0x02, 0x03, 0x05, 0x08, 0x13, 0x21}));

    inMap.put(createColSpec(17L, "num"), Bytes.toBytes(17L));
    inMap.put(createColSpec("a string", "whatevs"), Bytes.toBytes("a string"));
    inMap.put(createColSpec(ts, "exfiltrated"), Bytes.toBytes(ts.getTime()));
    inMap.put(createColSpec(new FileProxy(""), "Content"), Bytes.toBytes("some/bullshit/file.dat"));
    inMap.put(createColSpec(new BufferProxy(null), "Slack"), new byte[] {0x02, 0x03, 0x05, 0x08, 0x13, 0x21});

    populate(inMap, actualMap, actualStreams);
    assertEquals(expectedMap, actualMap);
    assertEquals(expectedStreams.size(), actualStreams.size());
    assertTrue(actualStreams.containsKey("Content"));
    assertTrue(actualStreams.containsKey("Slack"));
    assertEquals("some/bullshit/file.dat", ((FileProxy)actualStreams.get("Content")).getPath());
    InputStream str = actualStreams.get("Slack").open(new RawLocalFileSystem());
    byte[] tempBuf = new byte[6];
    assertEquals(6, str.read(tempBuf));
    assertEquals(0, str.available());
    assertArrayEquals(new byte[] {0x02, 0x03, 0x05, 0x08, 0x13, 0x21}, tempBuf);
  }
}
