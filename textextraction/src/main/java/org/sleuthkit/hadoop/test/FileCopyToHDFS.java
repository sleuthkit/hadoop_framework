package org.sleuthkit.hadoop.test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyToHDFS {
	  public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: FileCopyToHDFS <fromLocalFile> <toHDFSFile");
			System.exit(-1);
		}
	    Configuration conf = new Configuration();
		System.out.println("fs.default.name=" + conf.get("fs.default.name"));

	    String localSrc = args[0];
	    String dst = conf.get("fs.default.name") + args[1];	    
	    
	    InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
	    OutputStream out = fs.create(new Path(dst), new Progressable() {
	      public void progress() {
	        System. out.print(".");
	      }
	    });
	    
	    IOUtils.copyBytes(in, out, 4096, true);
	  }
	}