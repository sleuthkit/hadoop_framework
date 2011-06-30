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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.commons.io.IOUtils;

import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

public class MRCoffeeClient implements Closeable {

  public static class Result {
    public final String stdout;
    public final String stderr;

    public Result(String stdout, String stderr) {
      this.stdout = stdout;
      this.stderr = stderr;
    }
  }

  protected final Socket sock;

  protected DataInputStream in;
  protected DataOutputStream out;

  protected final StringBuilder stdout_sb = new StringBuilder();
  protected final StringBuilder stderr_sb = new StringBuilder();

  protected final byte[] buf = new byte[4096];

  public MRCoffeeClient() throws IOException {
    sock = AFUNIXSocket.newInstance();
  }

  static {
    final String libname = "libjunixsocket-linux-1.5-amd64.so";
    final String tmpdir = System.getProperty("java.io.tmpdir");
    final String src = '/' + libname;
    final File dst = new File(tmpdir + '/' + libname);

    if (!dst.exists()) {
      try {
        extract_lib(src, dst);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

//    System.load(dst.toString());
    System.setProperty("org.newsclub.net.unix.library.path", tmpdir);
  }
 
  protected static void extract_lib(String src, File dst) throws IOException {
    InputStream in = null;
    try {
      in = MRCoffeeJob.class.getResourceAsStream(src);
      if (in == null) {
        throw new IOException(src + " not found!");
      }

      OutputStream out = null;
      try {
        out = new FileOutputStream(dst);
        IOUtils.copy(in, out);
        out.close();
      }
      finally {
        IOUtils.closeQuietly(out);
      }

      in.close();
    }
    finally {
      IOUtils.closeQuietly(in);
    }
  }

  public void open(File pipe) throws IOException {
    sock.connect(new AFUNIXSocketAddress(pipe));
    in = new DataInputStream(sock.getInputStream());
    out = new DataOutputStream(sock.getOutputStream());
  }

  public DataOutputStream getOutputStream() {
    return out;
  }

  public void writeCommand(byte[] command) throws IOException {
    writeLength(command.length);
    writeData(command);
  }

  public void writeLength(long length) throws IOException {
    out.writeLong(length);
  }

  public void writeData(byte[] data, int offset, int length)
                                                           throws IOException {
    out.write(data, offset, length);
  }

  public void writeData(byte[] data) throws IOException {
    writeData(data, 0, data.length);
  }

  public Result readResult() throws IOException {
    // reset the buffers
    stdout_sb.setLength(0);
    stderr_sb.setLength(0);

    // read the reply, block by block
    boolean out_done = false, err_done = false;
    while (!out_done || !err_done) {
      // read source file descriptor for this block
      final int fd = in.readInt();

      // read length of block
      long len = in.readLong();

      // check whether one of the streams has ended
      if (len == 0) {
        if (fd == 1) {      // stdout
          out_done = true;
          continue;
        }
        else if (fd == 2) { // stderr
          err_done = true;
          continue;
        }          
      }

      final StringBuilder sb = fd == 1 ? stdout_sb : stderr_sb;

      // read the data block
      int size;
      for ( ; len > 0; len -= size) {
        size = (int) Math.min(buf.length, len);
        in.readFully(buf, 0, size);
        sb.append(new String(buf, 0, size));
      }
    }

    return new Result(stdout_sb.toString(), stderr_sb.toString());
  }

  public void close() throws IOException {
    boolean hadException = false;

    try {
      if (in != null) in.close();
      if (out != null) out.close();
      if (sock != null) sock.close();
    }
    catch (IOException e) {
      hadException = true;
      throw (IOException) new IOException().initCause(e);
    }
    finally {
      if (hadException) {
        IOUtils.closeQuietly(in);
        IOUtils.closeQuietly(out);
        IOUtils.closeQuietly(sock);
      }
    }
  }
}
