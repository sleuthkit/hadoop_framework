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

public class HDFSArchiver {

  protected static void traverse(FileSystem fs, Path p,
                                 ZipOutputStream zout, byte[] buf)
                                                           throws IOException {
    // chop off HDFS scheme and relativize to /
    String relpath = p.toUri().getPath();
    if (relpath.startsWith("/")) {
      relpath = relpath.substring(1);
    }

    final FileStatus pstat = fs.getFileStatus(p);
    if (pstat.isDir()) {
      // NB: dirs must end with '/'.
      final ZipEntry entry = new ZipEntry(relpath + '/');
      zout.putNextEntry(entry);
      zout.closeEntry();

      for (FileStatus stat : fs.listStatus(p)) {
        traverse(fs, stat.getPath(), zout, buf); 
      }
    }
    else {
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
  }

  public static void main(String[] argv) throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);

/*
    final String tp = "/texaspete/data/" + argv[0];

    final Path zpath = new Path(tp + "/reports.zip");
    final Path rpath = new Path(tp + "/reports");
*/

    final Path rpath = new Path(argv[0]);
    final Path zpath = new Path(argv[1]);

    final byte[] buf = new byte[4096];

    OutputStream out = null;
    try {
      out = fs.create(zpath);
      
      ZipOutputStream zout = null;
      try {
        zout = new ZipOutputStream(out);
        zout.setLevel(9);
        traverse(fs, rpath, zout, buf);
        zout.close();
      }
      finally {
        IOUtils.closeQuietly(zout); 
      }

      out.close();
    }
    finally {
      IOUtils.closeQuietly(out);
    }
  }
}
