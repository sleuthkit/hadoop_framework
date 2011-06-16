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

package com.lightboxtechnologies.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.io.input.ClosedInputStream;
import org.apache.commons.io.input.NullInputStream;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(JMock.class)
public class IOUtilsTest {
  protected final Mockery context = new JUnit4Mockery();

  @Test
  public void testCopyBuffer() throws IOException {
    final byte[] buf = new byte[1024];

    final byte[] expected = new byte[10000];
    final long seed = System.currentTimeMillis();
    final Random rng = new Random(seed);
    rng.nextBytes(expected);

    final ByteArrayInputStream in = new ByteArrayInputStream(expected);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    final int count = IOUtils.copy(in, out, buf);

    assertEquals("seed == " + seed, expected.length, count);
    assertArrayEquals("seed == " + seed, expected, out.toByteArray());
  }

  @Test
  public void testCopyLargeBuffer() throws IOException {
    final byte[] buf = new byte[1024];

    final byte[] expected = new byte[10000];
    final long seed = System.currentTimeMillis();
    final Random rng = new Random(seed);
    rng.nextBytes(expected);

    final ByteArrayInputStream in = new ByteArrayInputStream(expected);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    final long count = IOUtils.copyLarge(in, out, buf);

    assertEquals("seed == " + seed, expected.length, count);
    assertArrayEquals("seed == " + seed, expected, out.toByteArray());
  }

  @Test
  public void testCopyExactFull() throws IOException {
    final byte[] buf = new byte[1024];

    final byte[] expected = new byte[10000];
    final long seed = System.currentTimeMillis();
    final Random rng = new Random(seed);
    rng.nextBytes(expected);

    final ByteArrayInputStream in = new ByteArrayInputStream(expected);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    IOUtils.copy(in, out, buf, 10000);

    assertArrayEquals("seed == " + seed, expected, out.toByteArray());
  }

  @Test
  public void testCopyExactNotFull() throws IOException {
    final byte[] buf = new byte[1024];

    final byte[] expected = new byte[10000];
    final long seed = System.currentTimeMillis();
    final Random rng = new Random(seed);
    rng.nextBytes(expected);

    final ByteArrayInputStream in = new ByteArrayInputStream(expected);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    IOUtils.copy(in, out, buf, 9999);

    final byte[] actual = out.toByteArray();
    assertEquals("seed == " + seed, 9999, actual.length);
    for (int i = 0; i < actual.length; ++i) {
      assertEquals("seed == " + seed, expected[i], actual[i]);
    }
  }

  @Test(expected=IOException.class)
  public void testCopyExactTooLittle() throws IOException {
    final byte[] buf = new byte[1024];

    final byte[] expected = new byte[9999];
    final long seed = System.currentTimeMillis();
    final Random rng = new Random(seed);
    rng.nextBytes(expected);

    final ByteArrayInputStream in = new ByteArrayInputStream(expected);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    IOUtils.copy(in, out, buf, 10000);
  }

  @Test
  public void testCloseQuietlyCloseableOk() throws IOException {
    final Closeable c = context.mock(Closeable.class);

    context.checking(new Expectations() {
      {
        oneOf(c).close();
      }
    });

    IOUtils.closeQuietly(c);
  }

  @Test
  public void testCloseQuietlyCloseableThrows() throws IOException {
    final Closeable c = context.mock(Closeable.class);

    context.checking(new Expectations() {
      {
        oneOf(c).close(); will(throwException(new IOException()));
      }
    });

    IOUtils.closeQuietly(c);
  }

  @Test
  public void testCloseQuietlyCloseableNull() {
    IOUtils.closeQuietly((Closeable) null);
  }

  @Test
  public void testReadFull() throws IOException {
    final byte[] expected = new byte[100];
    Arrays.fill(expected, (byte) 1);

    final InputStream in = new ByteArrayInputStream(expected);
    final byte[] actual = new byte[100];
    final int count = IOUtils.read(in, actual);

    assertEquals(expected.length, count);
    assertArrayEquals(expected, actual);
  }

  @Test
  public void testReadShort() throws IOException {
    final byte[] expected = new byte[50];
    Arrays.fill(expected, (byte) 1);

    final InputStream in = new ByteArrayInputStream(expected);
    final byte[] actual = new byte[100];
    final int count = IOUtils.read(in, actual);

    assertEquals(50, count);
    assertArrayEquals(expected, Arrays.copyOfRange(actual, 0, count));
    assertArrayEquals(new byte[50], Arrays.copyOfRange(actual, count, 100));
  }

  @Test
  public void testReadNone() throws IOException {
    final InputStream in = new ByteArrayInputStream(new byte[0]);
    final byte[] buf = new byte[1];
    final int count = IOUtils.read(in, buf);

    assertEquals(-1, count);
    assertArrayEquals(new byte[1], buf);
  }

  @Test
  public void testReadClosed() throws IOException {
    final InputStream in = new ClosedInputStream();
    final byte[] buf = new byte[1];
    final int count = IOUtils.read(in, buf);

    assertEquals(-1, count);
    assertArrayEquals(new byte[1], buf);
  }
}
