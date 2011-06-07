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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.DecoderException;
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

public class ExtractMapper
       extends Mapper<NullWritable,FileSplit,ImmutableBytesWritable,KeyValue> {

  private static final Log LOG =
    LogFactory.getLog(ExtractMapper.class.getName());

  static enum FileTypes {
    SMALL,
    BIG,
    FRAGMENTED,
    BIG_DUPES,
    PROBLEMS,
    BYTES
  }

  private static final int SIZE_THRESHOLD = 10 * 1024 * 1024;

  private final ImmutableBytesWritable OutKey = new ImmutableBytesWritable();

  private final byte[] Buffer = new byte[SIZE_THRESHOLD];
  private MessageDigest MD5Hash,
                        SHA1Hash;

  private HTable EntryTbl;
  private HTable HashTbl;

  private final OutputStream NullStream = NullOutputStream.NULL_OUTPUT_STREAM;

  @Override
  protected void setup(Context context) throws IOException {
    LOG.info("Setup called");
    
    MD5Hash  = FsEntryUtils.getHashInstance("MD5");
    SHA1Hash = FsEntryUtils.getHashInstance("SHA1");

    // ensure that the hash table exists
    final Configuration conf = context.getConfiguration();

    final HBaseAdmin admin = new HBaseAdmin(conf);
    if (!admin.tableExists(HBaseTables.HASH_TBL_B)) {
      final HTableDescriptor tableDesc =
        new HTableDescriptor(HBaseTables.HASH_TBL_B);
      final HColumnDescriptor colFamDesc =
        new HColumnDescriptor(HBaseTables.HASH_COLFAM_B);
      colFamDesc.setCompressionType(Compression.Algorithm.GZ);
      tableDesc.addFamily(colFamDesc);
      admin.createTable(tableDesc);
    }
    else if (!admin.isTableEnabled(HBaseTables.HASH_TBL_B)) {
    	admin.enableTable(HBaseTables.HASH_TBL_B);
    }
    HashTbl = new HTable(conf, HBaseTables.HASH_TBL_B);

    EntryTbl =
      FsEntryHBaseOutputFormat.getHTable(context, HBaseTables.ENTRIES_TBL_B);
  }

  void extract(FSDataInputStream file, OutputStream outStream, Map<String,?> attrs, Context ctx) throws IOException {
    @SuppressWarnings("unchecked")
    final List<Map<String,?>> extents =
      (List<Map<String,?>>)attrs.get("extents");
    final long size = ((Number) attrs.get("size")).longValue();

    long read = 0;
    int bufOffset = 0,
        numExtents = 0;

    for (Map<String,?> dataRun : extents) {
      ++numExtents;
      long curAddr = ((Number) dataRun.get("addr")).longValue();
      final long length =
        Math.min(((Number) dataRun.get("len")).longValue(), size - read);
      final long endAddr = curAddr + length;

      int rlen;
      while (curAddr < endAddr) {
        // NB: endAddr - curAddr might be larger than 2^31-1, so we must
        // check that it doesn't overflow an int.
        rlen = Math.min(Buffer.length - bufOffset,
                 (int) Math.min(endAddr - curAddr, Integer.MAX_VALUE));


        // read the next chunk to the buffer

        // FIXME: Temporary workaround to prevent IndexOutOfBoundsExceptions from killing the ingest process.
        try {
          rlen = file.read(curAddr, Buffer, bufOffset, rlen);
        }
        catch (IndexOutOfBoundsException e) {
          throw new IllegalStateException("Balls!\ncurAddr == " + curAddr + ", Buffer.length == " + Buffer.length + ", bufOffset == " + bufOffset + ", rlen == " + rlen, e);
        }

        curAddr += rlen;
        bufOffset += rlen;

        if (bufOffset == Buffer.length) {
          // full buffer, flush it
          bufOffset = 0;
          outStream.write(Buffer, 0, Buffer.length);
          ctx.progress();
        }
      }

      read += length;
    }

    // flush the remaining bytes
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
      // FIXME: Temporary workaround to prevent IndexOutOfBoundsExceptions from killing the ingest process.
      try {
        extract(file, dout, map, context);
      }
      catch (IllegalStateException e) {
        LOG.warn(e);
        context.getCounter(FileTypes.PROBLEMS).increment(1);
      }
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
    LOG.info("Extracted small file of " + fileSize);

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

    final String hash = new String(Hex.encodeHex((byte[])rec.get("md5")));
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

  // column names
  private static final byte[] nsrl_col = "nsrl".getBytes();
  private static final byte[] bad_col = "bad".getBytes();

  protected void hash_lookup_and_mark(Map<String,Object> rec, String type)
                                                           throws IOException {
    final byte[] hash = (byte[]) rec.get(type);
    final Get request =
      new Get(hash).addColumn(HBaseTables.HASH_COLFAM_B, nsrl_col)
                   .addColumn(HBaseTables.HASH_COLFAM_B, bad_col);
    final Result result = HashTbl.get(request);
    
    if (!result.isEmpty()) {
      if (result.getValue(HBaseTables.HASH_COLFAM_B, nsrl_col) != null) {
        // My hash is in the NSRL.
        rec.put("nsrl", 1);
      }

      if (result.getValue(HBaseTables.HASH_COLFAM_B, bad_col) != null) {
        // I've been a very naughty file.
        rec.put("bad", 1);
      }
    }
  }

  protected static final byte[] ingest_col = "ingest".getBytes();

  protected static final byte[] one = { 1 };

  protected void process_extent(FSDataInputStream file, FileSystem fs, Path outPath, Map<String,?> map, Context context) throws IOException, InterruptedException {
    String id = (String)map.get("id");
    try {
      OutKey.set(Hex.decodeHex(id.toCharArray()));
    }
    catch (DecoderException e) {
      throw new RuntimeException(e);
    }
    final long fileSize = ((Number) map.get("size")).longValue();
    StringBuilder sb = new StringBuilder("Extracting ");
    sb.append(id);
    sb.append(":");
    sb.append((String)map.get("fp"));
    sb.append(" (");
    sb.append(fileSize);
    sb.append(" bytes)");
    LOG.info(sb.toString());
    MD5Hash.reset();

    final Map<String,Object> rec = fileSize > SIZE_THRESHOLD ?
      process_extent_large(file, fs, outPath, map, context) :
      process_extent_small(file, fileSize, map, context);

    // check if the md5 is known
    hash_lookup_and_mark(rec, "md5");

    // check if the sha1 is known
    hash_lookup_and_mark(rec, "sha1");

    // write the entry to the file table
    EntryTbl.put(
      FsEntryHBaseOutputFormat.FsEntryHBaseWriter.createPut(
        OutKey.get(), rec, Bytes.toBytes("core")
      )
    );

    // write the key for the hash table
    final long timestamp =  System.currentTimeMillis();
    final KeyValue OutValue = new KeyValue(
      OutKey.get(), HBaseTables.HASH_COLFAM_B, ingest_col, timestamp, one
    );
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
    catch (IOException io) {
      throw io;
    }
    catch (InterruptedException interrupt) {
      throw interrupt;
    }
    catch (Exception e) {
      LOG.error("Extraction exception " + e);
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
