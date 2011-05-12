package com.lightboxtechnologies.io;

import java.io.Closeable;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * General I/O stream manipulation utilities. This class provides static
 * utility methods to reduce boilerplate I/O code.
 *
 * @author Joel Uckelman
 */
public class IOUtils extends org.apache.commons.io.IOUtils {
  protected IOUtils() {}

  /**
   * Copies bytes from an <code>InputStream</code> to an
   * <code>OutputStream</code> via a <code>byte</code> buffer. This
   * method buffers input internally, so the input stream should not
   * be a <code>BufferedInputStream</code>.
   *
   * @param in the source
   * @param out the destination
   * @param buffer the buffer
   * @return the number of bytes copied
   * @throws IOException if one occurs while reading or writing
   */
  public static int copy(InputStream in, OutputStream out, byte[] buffer)
                                                           throws IOException {
    final long count = copyLarge(in, out, buffer);
    return count > Integer.MAX_VALUE ? -1 : (int) count;
  }

  /**
   * Copies bytes from a large (over 2GB) <code>InputStream</code> to an
   * <code>OutputStream</code> via a <code>byte</code> buffer. This
   * method buffers input internally, so the input stream should not
   * be a <code>BufferedInputStream</code>.
   *
   * @param in the source
   * @param out the destination
   * @param buffer the buffer
   * @return the number of bytes copied
   * @throws IOException if one occurs while reading or writing
   */
  public static long copyLarge(InputStream in, OutputStream out, byte[] buffer)
                                                           throws IOException {
    long count = 0;
    int n = 0;
    while ((n = in.read(buffer)) != -1) {
      out.write(buffer, 0, n);
      count += n;
    }
    return count;
  }

  /**
   * Close a {@link Closeable} unconditionally. Equivalent to
   * calling <code>c.close()</code> when <code>c</code> is nonnull.
   * {@link IOException}s are swallowed, as there is generally
   * nothing that can be done about exceptions on closing.
   *
   * @param c a (possibly <code>null</code>) <code>Closeable</code>
   */
  public static void closeQuietly(Closeable c) {
    if (c == null) return;

    try {
      c.close();
    }
    catch (IOException e) {
      // ignore
    }
  }

  /**
   * Reads from an {@link InputStream} to a byte array. This will always
   * completely fill the byte array, unless there are no more bytes to
   * read from the stream.
   *
   * @param in the input stream from which to read
   * @param buf the byte array to fill
   * @return the number of bytes read, of <code>-1</code> if at the end of
   * the stream
   *
   * @throws IOException if one occurs while reading
   */
  public static int read(InputStream in, byte[] buf) throws IOException {
    int num;
    int off = 0;
    while (off < buf.length &&
            (num = in.read(buf, off, buf.length-off)) != -1) {
      off += num;
    }

    // This will read at least one byte if there are any to be read,
    // so bytes read cannot be zero.
    return off == 0 ? -1 : off;
  }
}
