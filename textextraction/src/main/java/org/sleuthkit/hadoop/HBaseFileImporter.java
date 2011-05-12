/*
   Copyright 2011 Basis Technology Corp.

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

package org.sleuthkit.hadoop;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;

public class HBaseFileImporter extends Configured implements Tool, Visitor<File> {
	static final byte [] DATA_FAMILY = Bytes.toBytes("data");
	static final byte [] PATH_QUAL = Bytes.toBytes("path");
	static final byte [] HASH_QUAL = Bytes.toBytes("hash");
	static final byte [] CONT_QUAL = Bytes.toBytes("cont");

	static final byte [] INFO_QUAL = Bytes.toBytes("info");
	static final byte [] INFO_VAL_GOOD = Bytes.toBytes("g");
	static final byte [] INFO_VAL_BAD = Bytes.toBytes("b");

	static final long MAX_IN_REC = 256*1024;	//256K
	//this prop value changes depending on distribution mode (local, pseudo, etc.)
	public static final String HDFS_PROP_NAME = "fs.default.name";
	public static final String HDFS = "hdfs://";

	Configuration conf;
	String prefixHDFS;
	HTable fileTable;
	HTable hashTable;

	public HBaseFileImporter() throws Exception {
		conf = new Configuration();
		prefixHDFS = conf.get(HDFS_PROP_NAME);
		if (prefixHDFS == null) {
			throw new Exception(HDFS_PROP_NAME + " is not set in configuration");
		}
		System.out.println(HDFS_PROP_NAME + "=" + prefixHDFS);
	}
	public void visit(File node) {
		// get file info - should be called only with files (not directory)
		if (node.isDirectory())
			assert (false);

		FileInputStream fis = null;
		try {
			//calculate MD5 hash
			fis = new FileInputStream(node);
			MD5Hash md5 = MD5Hash.digest(fis);
			//we must make sure stream is at the start, so we close it and reopen if we need
			fis.close();
			fis = null;
			boolean fileSeen = addHashTableRec(md5, prefixHDFS + node.getPath());
			addFileTableRec(fileSeen, node, md5);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		finally {
			if (fis == null)
				return;
			try {
				fis.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	//create an HBase record for a file
	private void addFileTableRec(boolean fileSeen, File file, MD5Hash md5) throws IOException {
		String filePathHDFS = prefixHDFS + file.getPath();
		Put p = new Put(Bytes.toBytes(filePathHDFS));
		p.add(DATA_FAMILY, HASH_QUAL, md5.getDigest());

		if (!fileSeen) {
			//if file is large - store it in HDFS and use it's HDFS path
			if (file.length() > MAX_IN_REC) {
				FileSystem fs = FileSystem.get(conf);
				if (prefixHDFS.startsWith(HDFS)) { //pseudo or fully distributed - got to copy file
					System.out.println("copying file to: " + filePathHDFS);
					fs.copyFromLocalFile(new Path(file.getPath()), new Path(filePathHDFS));
				}
				else {	//local - don't need to copy
					System.out.println("NOT copying file: " + file.getPath());
				}
				//store HDFS path with HDFS prefix
				p.add(DATA_FAMILY, PATH_QUAL, Bytes.toBytes(filePathHDFS));
			}
			else {
				//store file in "content" column
				p.add(DATA_FAMILY, CONT_QUAL, getBytesFromFile(file));
			}
		}
		fileTable.put(p);
	}
	//return true if we've seen the file before, false - not seen
	private boolean addHashTableRec(MD5Hash md5, String path) throws IOException {
		//in this hashTable first row for each hash value may have INFO_QUAL column value set to INFO_VAL_GOOD or INFO_VAL_BAD
		byte[] key = md5.getDigest();
		boolean exists = hashTable.exists(new Get(key));

		Put p = new Put(key);
		p.add(DATA_FAMILY, PATH_QUAL, Bytes.toBytes(path));
		hashTable.put(p);
		return exists;
	}
	public static byte[] getBytesFromFile(File file) throws IOException {
        InputStream is = new FileInputStream(file);

        // Get the size of the file
        long length = file.length();

        if (length > Integer.MAX_VALUE) {
            // File is too large
        	assert(false);
        }
        // Create the byte array to hold the data
        byte[] bytes = new byte[(int)length];

        // Read in the bytes
        int offset = 0;
        int numRead = 0;
        while (offset < bytes.length
               && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
            offset += numRead;
        }

        // Ensure all the bytes have been read in
        if (offset < bytes.length) {
            throw new IOException("Could not completely read file " + file.getName());
        }

        // Close the input stream and return bytes
        is.close();
        return bytes;
    }
	public int run(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("Usage: HBaseFileImporter <file/dir path>");
			return -1;
		}
		Configuration cf = HBaseConfiguration.create();
		fileTable = new HTable(cf, "fileTable");
		hashTable = new HTable(cf, "hashTable");

		FileProcessor fp = new FileProcessor(this);
		//TODO: first we need to make sure that "HD image Identifier" has not been seen yet, as we'll rely on it's uniqueness
		fp.process(new File(args[0]));
		return 0;
	}
	public static void main(String[] args) throws Exception {
		Configuration cf = HBaseConfiguration.create();
		int exitCode = ToolRunner.run(cf, new HBaseFileImporter(), args);
		System.exit(exitCode);
	}

	public static int runPipeline(String importPath) throws Exception{
	       Configuration cf = HBaseConfiguration.create();
	       return ToolRunner.run(cf, new HBaseFileImporter(), new String[] {importPath});
	}
}