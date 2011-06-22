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

import com.lightboxtechnologies.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.DecoderException;

import java.io.*;

import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Uploader {
  public static void main(String[] args) throws Exception {
    final Configuration conf = new Configuration();
    final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 1) {
      System.err.println("Usage: Uploader <dest path>");
      System.err.println("Writes data to HDFS path from stdin");
      System.exit(2);
    }

    MessageDigest hasher = FsEntryUtils.getHashInstance("MD5");
    DigestInputStream hashedIn = new DigestInputStream(System.in, hasher);

    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(otherArgs[0]);
    FSDataOutputStream outFile = fs.create(path, true);

    IOUtils.copyLarge(hashedIn, outFile, new byte[1024 * 1024]);

    System.out.println(new String(Hex.encodeHex(hasher.digest())));
  }
}
