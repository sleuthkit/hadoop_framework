package com.lightboxtechnologies.spectrum;

import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;

public class ExtentInputStream extends InputStream {

  protected final FSDataInputStream in;

  protected final Iterator<Map<String,?>> ext_iter;

  protected Map<String,?> cur_extent = null;
  protected long cur_pos;
  protected long cur_end;

  protected long remaining;

  public ExtentInputStream(FSDataInputStream in,
                           List<Map<String,?>> extents, long length) {
    this.in = in;
    this.ext_iter = extents.iterator();
    this.remaining = length;
  }
 
  protected boolean prepareExtent() {
    // prepare next extent
    if (cur_extent == null || cur_pos == cur_end) {
      if (ext_iter.hasNext()) {
        cur_extent = ext_iter.next();

        final long length =
          Math.min(((Number) cur_extent.get("len")).longValue(), remaining);

        cur_pos = ((Number) cur_extent.get("addr")).longValue();
        cur_end = cur_pos + length;
      }
      else {
        return false;
      }
    }

    return true;
  }

  @Override
  public int read() throws IOException {
    if (!prepareExtent() || remaining <= 0) {
      return -1;
    }

    --remaining;
    final byte[] b = new byte[1];
    return in.read(cur_pos++, b, 0, 1);
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    if (!prepareExtent() || remaining <= 0) {
      return -1;
    }

    // NB: cur_end - cur_pos might be larger than 2^31-1, so we must
    // check that it doesn't overflow an int.
    int rlen = Math.min(len,
      (int) Math.min(cur_end - cur_pos, Integer.MAX_VALUE));

    rlen = in.read(cur_pos, buf, off, rlen);
    cur_pos += rlen;
    remaining -= rlen;

    return rlen;
  }

/*
  @Override
  public long skip(long n) throws IOException {
    
  }
*/

  @Override
  public int available() throws IOException {
    return (int) Math.min(remaining, Integer.MAX_VALUE);
  }

/*
  @Override
  public void close() throws IOException {
  }
*/
} 
