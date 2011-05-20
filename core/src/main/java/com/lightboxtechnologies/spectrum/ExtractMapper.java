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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.lightboxtechnologies.io.IOUtils;

public class ExtractMapper extends Mapper<NullWritable,FileSplit,Text,Text> {

  public static final Log LOG = LogFactory.getLog(ExtractMapper.class.getName());

  static enum FileTypes {
    SMALL,
    BIG,
    FRAGMENTED,
    BIG_DUPES,
    PROBLEMS,
    BYTES
  }

  private static final int SIZE_THRESHOLD = 10 * 1024 * 1024;

  private Text OutKey = new Text();
  private Text OutValue = new Text();
  private final byte[] Buffer = new byte[SIZE_THRESHOLD];
  private MessageDigest MD5Hash,
                        SHA1Hash;
  private HTable EntryTbl;
  private final OutputStream NullStream = NullOutputStream.NULL_OUTPUT_STREAM;

  private MessageDigest getHashInstance(final String alg) {
    MessageDigest hash;
    try {
      hash = MessageDigest.getInstance(alg);
    }
    catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException("As if " + alg + " isn't going to be implemented, bloody Java tossers");
    }
    return hash;
  }

  @Override
  protected void setup(Context context) throws IOException {
    LOG.info("Setup called");
    
    MD5Hash  = getHashInstance("MD5");
    SHA1Hash = getHashInstance("SHA1");
    EntryTbl = FsEntryHBaseOutputFormat.getHTable(context, Bytes.toBytes("core"));
  }

  void extract(FSDataInputStream file, OutputStream outStream, Map<String,?> attrs, Context ctx) throws IOException {
    @SuppressWarnings("unchecked")
    final List<Map<String,?>> extents =
      (List<Map<String,?>>)attrs.get("extents");
    final long size = (Long)attrs.get("size");

    long read = 0;
    int bufOffset = 0,
        numExtents = 0;

    for (Map<String,?> dataRun : extents) {
      ++numExtents;
      long curAddr = (Long)dataRun.get("addr");
      final long length = Math.min((Long)dataRun.get("len"), size - read),
                endAddr = curAddr + length;
      int len = 0;
      while (curAddr < endAddr) {
// FIXME: dodgy math: could endAddr - curAddr be negative if cast to an int?
        len = file.read(curAddr, Buffer, bufOffset, (int)Math.min(Buffer.length - bufOffset, endAddr - curAddr));
        curAddr += len;
        bufOffset += len;
        if (bufOffset == Buffer.length) {
          bufOffset = 0;
          outStream.write(Buffer, 0, Buffer.length);
          ctx.progress();
        }
      }
      read += length;
    }
    outStream.write(Buffer, 0, bufOffset);
    outStream.flush();

    if (numExtents > 1) {
      ctx.getCounter(FileTypes.FRAGMENTED).increment(1);
    }

    ctx.getCounter(FileTypes.BYTES).increment(read);
    if (read != size) {
      LOG.warn("problem reading " + (String)attrs.get("id") + ". read = " + read + "; size = " + size);
      ctx.getCounter(FileTypes.PROBLEMS).increment(1);
    }
  }

  LongWritable seekToMapBlock(SequenceFile.Reader extents, long startOffset) throws IOException {
    final LongWritable cur = new LongWritable();
    while (extents.next(cur)) {
      if (cur.get() >= startOffset) {
        return cur;
      }
    }
    return null;
  }

  SequenceFile.Reader openExtentsFile(FileSystem hdpFs, Configuration conf) throws IOException {
    SequenceFile.Reader extents = null;

    final Path[] files = DistributedCache.getLocalCacheFiles(conf);
    if (files != null && files.length > 0) {
      final LocalFileSystem localfs = FileSystem.getLocal(conf);
      LOG.info("Opening extents file " + files[0]);
      extents = new SequenceFile.Reader(localfs, files[0], conf);
    }
    else if (files == null) {
      throw new RuntimeException("No file paths retrieved from distributed cache");
      // extents = new SequenceFile.Reader(hdpFs, new Path("ceic_extents/part-r-00000"), conf); // TO-DO: fix hard-coding
    }

    if (extents == null) {
      throw new RuntimeException("Could not open extents file. Number of files in the cache: " + files.length);
    }

    return extents;
  }

  protected void hashAndExtract(final Map<String,Object> rec, OutputStream out, FSDataInputStream file, Map<String,?> map, Context context) throws IOException {
    MD5Hash.reset();
    SHA1Hash.reset();
    OutputStream dout = null;
    try {
      dout = new DigestOutputStream(new DigestOutputStream(out, MD5Hash), SHA1Hash);
      extract(file, dout, map, context);
    }
    finally {
      IOUtils.closeQuietly(dout);
    }
    rec.put("md5", MD5Hash.digest());
    rec.put("sha1", SHA1Hash.digest());
  }

  protected Map<String,Object> process_extent_small(FSDataInputStream file, long fileSize, Map<String,?> map, Context context) throws IOException {
    context.getCounter(FileTypes.SMALL).increment(1);

    final Map<String,Object> rec = new HashMap<String,Object>();
    hashAndExtract(rec, NullStream, file, map, context);

    // FIXME: makes a second copy; would be nice to give
    // Put a portion of Buffer; can probably do this with
    // java.nio.Buffer.
    final StreamProxy content = new BufferProxy(Arrays.copyOf(Buffer, (int)fileSize));

    rec.put("Content", content);
    return rec;
  }

  protected Map<String,Object> process_extent_large(FSDataInputStream file, FileSystem fs, Path outPath, Map<String,?> map, Context context) throws IOException {
    context.getCounter(FileTypes.BIG).increment(1);

    final Map<String,Object> rec = new HashMap<String,Object>();

    OutputStream fout =  null;
    try {
      fout = fs.create(outPath, true);
      hashAndExtract(rec, fout, file, map, context);
    }
    finally {
      IOUtils.closeQuietly(fout);
    }

    String hash = new String(Hex.encodeHex((byte[])rec.get("md5")));
    final Path subDir = new Path("ev", hashFolder(hash)),
          hashPath = new Path(subDir, hash);
    fs.mkdirs(subDir);

    if (fs.exists(hashPath)) {
      context.getCounter(FileTypes.BIG_DUPES).increment(1);
    }
    else if (!fs.rename(outPath, hashPath)) {
      LOG.warn("Could not rename " + outPath + " to " + hashPath);
      context.getCounter(FileTypes.PROBLEMS).increment(1);
    }
    final StreamProxy content = new FileProxy(hashPath.toString());
    rec.put("Content", content);
    return rec;
  }

  protected void process_extent(FSDataInputStream file, FileSystem fs, Path outPath, Map<String,?> map, Context context) throws IOException, InterruptedException {
    final String id = (String)map.get("id");
    final long fileSize = (Long)map.get("size");
    MD5Hash.reset();

    final Map<String,Object> rec = fileSize > SIZE_THRESHOLD ?
      process_extent_large(file, fs, outPath, map, context) :
      process_extent_small(file, fileSize, map, context);

    EntryTbl.put(
      FsEntryHBaseOutputFormat.FsEntryHBaseWriter.createPut(
        id, rec, Bytes.toBytes("core")
      )
    );

    OutKey.set(id);
    OutValue.set(new String(Hex.encodeHex((byte[])rec.get("md5"))));
    context.write(OutKey, OutValue);
  }

  protected int process_extents(FileSystem fs, Path path, SequenceFile.Reader extents, LongWritable offset, long endOffset, Context context) throws IOException, InterruptedException {

    int numFiles = 0;
    long cur = offset.get();

    final JsonWritable attrs = new JsonWritable();
    final Path outPath = new Path("ev/tmp", UUID.randomUUID().toString());

    FSDataInputStream file = null;
    try {
      file = fs.open(path, 2 * Buffer.length);
      extents.getCurrentValue(attrs);

      do {
        ++numFiles;

        @SuppressWarnings("unchecked")
        final Map<String,?> map = (Map<String,?>)attrs.get();
        process_extent(file, fs, outPath, map, context);

      } while (extents.next(offset, attrs) && (cur = offset.get()) < endOffset);
    }
    finally {
      IOUtils.closeQuietly(file);
    }

    return numFiles;
  }

  @Override
  protected void map(NullWritable k, FileSplit split, Context context)
                                     throws IOException, InterruptedException {
    final long startOffset = split.getStart(),
                 endOffset = startOffset + split.getLength();
    LOG.info("startOffset = " + startOffset + "; endOffset = " + endOffset);
    context.setStatus("Offset " + startOffset);

    final Configuration conf = context.getConfiguration();
    final FileSystem fs = FileSystem.get(conf);

    int numFiles = 0;

    SequenceFile.Reader extents = null;
    try {
      extents = openExtentsFile(fs, conf);
      final LongWritable offset = seekToMapBlock(extents, startOffset);

      if (offset != null && offset.get() < endOffset) {
        numFiles = process_extents(
          fs, split.getPath(), extents, offset, endOffset, context
        );
      }

      LOG.info("This split had " + numFiles + " files in it");
      extents.close();
    }
    finally {
      IOUtils.closeQuietly(extents);
    }
  }

  public static String hashFolder(String hash) {
    final StringBuilder hashPathBuf = new StringBuilder();
    hashPathBuf.setLength(0);
    hashPathBuf.append(hash.substring(0, 2));
    hashPathBuf.append("/");
    hashPathBuf.append(hash.substring(2, 4));
    return hashPathBuf.toString();
  }
}
