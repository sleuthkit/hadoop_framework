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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.lightboxtechnologies.io.IOUtils;

/**
 * Writes a directory tree in HDFS to a ZIP archive.
 *
 * @author Joel Uckelman
 */
public class HDFSArchiver {

  protected static String relativize(Path p) {
    // chop off HDFS scheme and relativize to /
    String relpath = p.toUri().getPath();
    if (relpath.startsWith("/")) {
      relpath = relpath.substring(1);
    }

    return relpath;
  }

  protected static void handleDirectory(
    String relpath, FileSystem fs, Path path, ZipOutputStream zout, byte[] buf)
                                                           throws IOException {
    // NB: dirs must end with '/'. If we don't add dirs, then only
    // dirs which also have files in them will be created when the
    // archive is unzipped.
    final ZipEntry entry = new ZipEntry(relpath + '/');
    zout.putNextEntry(entry);
    zout.closeEntry();

    for (FileStatus stat : fs.listStatus(path)) {
      traverse(fs, stat.getPath(), zout, buf);
    }
  }

  protected static void handleFile(
    String relpath, FileSystem fs, Path p, ZipOutputStream zout, byte[] buf)
                                                           throws IOException {
    final ZipEntry entry = new ZipEntry(relpath);
    zout.putNextEntry(entry);

    InputStream in = null;
    try {
      in = fs.open(p);
      IOUtils.copy(in, zout, buf);
      in.close();
    }
    finally {
      IOUtils.closeQuietly(in);
    }

    zout.closeEntry();
  }

  protected static void traverse(FileSystem fs, Path p,
                                 ZipOutputStream zout, byte[] buf)
                                                           throws IOException {
    final String relpath = relativize(p);

    final FileStatus pstat = fs.getFileStatus(p);
    if (pstat.isDir()) {
      handleDirectory(relpath, fs, p, zout, buf);
    }
    else {
      handleFile(relpath, fs, p, zout, buf);
    }
  }

  protected static void zip(FileSystem fs, Path src, OutputStream out)
                                                           throws IOException {
    final byte[] buf = new byte[4096];

    ZipOutputStream zout = null;
    try {
      zout = new ZipOutputStream(out);
      zout.setLevel(9);
      traverse(fs, src, zout, buf);
      zout.close();
    }
    finally {
      IOUtils.closeQuietly(zout);
    }
  }

  public static int runPipeline(String src, String dst) throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);

    final Path rpath = new Path(src);
    final Path zpath = new Path(dst);

    if (!fs.exists(rpath)) {
      throw new IOException("Source path does not exist.");
    }

    OutputStream out = null;
    try {
      out = zpath.getFileSystem(conf).create(zpath);
      zip(fs, rpath, out);
      out.close();
    }
    finally {
      IOUtils.closeQuietly(out);
    }

    return 0;
  }

  public static void main(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.err.println("Usage: HDFSArchiver <indir> <outfile>");
      System.exit(2);
    }

    System.exit(runPipeline(argv[0], argv[1]));
  }
}
