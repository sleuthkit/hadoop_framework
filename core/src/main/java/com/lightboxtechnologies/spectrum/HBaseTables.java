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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HBaseTables {

  private HBaseTables() {}

  private static final Log LOG = LogFactory.getLog(HBaseTables.class);

  // name for Conf objects
  public static final String ENTRIES_TBL_VAR = "com.lbt.htable";

  static public final String ENTRIES_TBL = "entries";
  static public final byte[] ENTRIES_TBL_B = Bytes.toBytes(ENTRIES_TBL);

  static public final String ENTRIES_COLFAM = "core";
  static public final byte[] ENTRIES_COLFAM_B = Bytes.toBytes(ENTRIES_COLFAM);

  static public final String HASH_TBL = "hash";
  static public final byte[] HASH_TBL_B = Bytes.toBytes(HASH_TBL);

  static public final String HASH_COLFAM = "0";
  static public final byte[] HASH_COLFAM_B = Bytes.toBytes(HASH_COLFAM);

  static public final String IMAGES_TBL = "images";
  static public final byte[] IMAGES_TBL_B = Bytes.toBytes(IMAGES_TBL);

  static public final String IMAGES_COLFAM = "0";
  static public final byte[] IMAGES_COLFAM_B = Bytes.toBytes(IMAGES_COLFAM);

  public static HTable summon(Configuration conf, byte[] tname, byte[] cfam)
                                                           throws IOException {
    final HBaseAdmin admin = new HBaseAdmin(conf);
    if (!admin.tableExists(tname)) {
      final HTableDescriptor tableDesc = new HTableDescriptor(tname);
      if (!tableDesc.hasFamily(cfam)) {
        final HColumnDescriptor colFamDesc = new HColumnDescriptor(cfam);
        colFamDesc.setCompressionType(Compression.Algorithm.GZ);
        tableDesc.addFamily(colFamDesc);
      }

      try {
        admin.createTable(tableDesc);
      }
      catch (TableExistsException e) {
        LOG.info("Tried to create the hash table, but it already exists. Probably just lost a race condition.");
      }
    }
    else if (!admin.isTableEnabled(tname)) {
    	admin.enableTable(tname);
    }

    return new HTable(conf, tname);
  }
}
