package com.lightboxtechnologies.spectrum;

import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;

@RunWith(JMock.class)
public class ExtentsInputStreamTest {

  protected final Mockery context = new JUnit4Mockery() {
    {
      setImposteriser(ClassImposteriser.INSTANCE);
    }
  };

  protected class ArrayFSDataInputStream extends FSDataInputStream {
    protected final byte[] src;

    public ArrayFSDataInputStream(byte[] src) throws IOException {
      super(context.mock(FSDataInputStream.class));
      this.src = src;
    }

    @Override
    public void close() {}

    @Override
    public int read(long pos, byte[] buf, int off, int len) {
      final int rlen = Math.min(src.length - (int) pos, len);
      System.arraycopy(src, (int) pos, buf, off, rlen);
      return rlen;
    }
  }

  @Test
  public void readArrayNoExtentsTest() throws IOException {
    final List<Map<String,Object>> extents = Collections.emptyList();
    
    final FSDataInputStream in = new ArrayFSDataInputStream(new byte[0]);
   
    final InputStream eis = new ExtentsInputStream(in, extents);

    final byte[] buf = { 17 };

    assertEquals(-1, eis.read(buf, 0, buf.length));
    assertEquals(17, buf[0]);
  }

  @Test
  public void readByteNoExtentsTest() throws IOException {
    final List<Map<String,Object>> extents = Collections.emptyList();
    
    final FSDataInputStream in = new ArrayFSDataInputStream(new byte[0]);

    final InputStream eis = new ExtentsInputStream(in, extents);

    assertEquals(-1, eis.read());
  }

  @Test
  public void readArrayOneExtentTest() throws IOException {
    final Map<String,Object> extent = new HashMap<String,Object>();
    extent.put("addr", Long.valueOf(42));
    extent.put("len", Long.valueOf(10));
    
    final List<Map<String,Object>> extents = Collections.singletonList(extent);
 
    final byte[] src = new byte[1024]; 
    for (int i = 0; i < src.length; ++i) {
      src[i] = (byte) (i % 256);
    }

    final byte[] expected = new byte[10];
    System.arraycopy(src, 42, expected, 0, 10);

    final FSDataInputStream in = new ArrayFSDataInputStream(src);

    final InputStream eis = new ExtentsInputStream(in, extents);

    final byte[] actual = new byte[expected.length];
    assertEquals(10, eis.read(actual, 0, actual.length));
    assertArrayEquals(expected, actual);
  }

  @Test
  public void readIntOneExtentTest() throws IOException {
    final Map<String,Object> extent = new HashMap<String,Object>();
    extent.put("addr", Long.valueOf(42));
    extent.put("len", Long.valueOf(10));
    
    final List<Map<String,Object>> extents = Collections.singletonList(extent);

    final byte[] src = new byte[1024]; 
    for (int i = 0; i < src.length; ++i) {
      src[i] = (byte) (i % 256);
    }

    final FSDataInputStream in = new ArrayFSDataInputStream(src);

    final InputStream eis = new ExtentsInputStream(in, extents);

    assertEquals(src[42], eis.read());
  }

  @Test
  public void readArrayMultipleExtentsTest() throws IOException {
    final int n = 10;

    final byte[] src = new byte[2*n];
    for (int i = 0; i < src.length; ++i) {
      src[i] = (byte) (i % 256);
    }

    final List<Map<String,Object>> extents =
      new ArrayList<Map<String,Object>>();

    for (int i = 0, off = 0; i <= n; off += i, ++i) {
      final Map<String,Object> extent = new HashMap<String,Object>();
      extent.put("addr", Long.valueOf(i));
      extent.put("len", Long.valueOf(i));
      extents.add(extent);
    }      

    final FSDataInputStream in = new ArrayFSDataInputStream(src);

    final InputStream eis = new ExtentsInputStream(in, extents);

    for (int i = 0; i < extents.size(); ++i) {
      final int rlen = ((Number) extents.get(i).get("len")).intValue();
      
      final byte[] actual = new byte[rlen];
      assertEquals(rlen, eis.read(actual, 0, rlen));

      final byte[] expected = new byte[rlen];
      final int addr = ((Number) extents.get(i).get("addr")).intValue();
      System.arraycopy(src, addr, expected, 0, rlen);

      assertArrayEquals(expected, actual);
    }
  }
}
