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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.commons.io.IOUtils;

/**
 * Stuff fsrip info output into the images table in HBase.
 *
 * @author Joel Uckelman
 */
public class InfoPutter {

  public static void main(String[] args) throws IOException {
    final Configuration conf = HBaseConfiguration.create();

    final String[] otherArgs =
      new GenericOptionsParser(conf, args).getRemainingArgs();

    final String imageID = otherArgs[0];
    final String friendlyName = otherArgs[1];

    HTable imgTable = null;

    try {
      imgTable = HBaseTables.summon(
        conf, HBaseTables.IMAGES_TBL_B, HBaseTables.IMAGES_COLFAM_B
      );

      // check whether the image ID is in the images table
      final byte[] hash = Bytes.toBytes(imageID);

      final Get get = new Get(hash);
      final Result result = imgTable.get(get);

      if (result.isEmpty()) {
        // row does not exist, add it
      
        final byte[] friendly_col = "friendly_name".getBytes();
        final byte[] json_col = "json".getBytes();

        final byte[] friendly_b = friendlyName.getBytes();
        final byte[] json_b = IOUtils.toByteArray(System.in);

        final Put put = new Put(hash);
        put.add(HBaseTables.IMAGES_COLFAM_B, friendly_col, friendly_b);
        put.add(HBaseTables.IMAGES_COLFAM_B, json_col, json_b);

        imgTable.put(put);

        System.exit(0);
      }
      else {
        // row exists, fail!
        System.exit(1);
      }
    }
    finally {
      imgTable.close();
    }
  }
}
