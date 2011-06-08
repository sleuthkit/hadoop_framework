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

package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.codec.binary.Hex;

import com.lightboxtechnologies.spectrum.HBaseTables;
import com.lightboxtechnologies.spectrum.KeyUtils;

import static com.lightboxtechnologies.nsrl.HashLoader.HashLoaderMapper;

/**
 * @author Joel Uckelman
 */
@RunWith(JMock.class)
public class HashLoaderHelperTest {
  protected final Mockery context = new JUnit4Mockery() {
    {
      setImposteriser(ClassImposteriser.INSTANCE);
    }
  }; 

  protected void writeRowTester(final boolean sha1rowkey) throws Exception {
    final long timestamp = 1234567890;
        
    final ProdData pd = new ProdData(
      42,
      "ACME Roadrunner Decapitator",
      "1.0",
      "1",
      "ACME", 
      "Meep",
      "roadrunner exterminator"
    );

    final MfgData md = new MfgData("ACME", "ACME Corporation");

    final Hex hex = new Hex(); 
    final byte[] sha1 =
      (byte[]) hex.decode("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    final byte[] md5 =
      (byte[]) hex.decode("deadbeefdeadbeefdeadbeefdeadbeef");
    final byte[] crc32 = (byte[]) hex.decode("deadbeef");

    final byte[] sha1_col = "sha1".getBytes();
    final byte[] md5_col = "md5".getBytes();
    final byte[] crc32_col = "crc32".getBytes();
    final byte[] size_col = "filesize".getBytes();
    final byte[] nsrl_col = "NSRL".getBytes();

    final byte[] family = HBaseTables.HASH_COLFAM_B;

    final byte[] key = sha1rowkey ? sha1 : md5;
    final byte ktype = (byte) (sha1rowkey ? 1 : 0);

    final HashData hd = new HashData(
      sha1, md5, crc32, "librrkill.so.1", 123456, pd.code, "ACME_OS", "special"
    );

    final ImmutableBytesWritable okey_crc32 = new ImmutableBytesWritable(
      KeyUtils.makeEntryKey(key, ktype, crc32_col)
    );
    final KeyValue kv_crc32 =
      new KeyValue(key, family, crc32_col, timestamp, crc32);    

    final ImmutableBytesWritable okey_sha1 = new ImmutableBytesWritable(
      KeyUtils.makeEntryKey(key, ktype, sha1_col)
    );
    final KeyValue kv_sha1 =
      new KeyValue(key, family, sha1_col, timestamp, sha1);    

    final ImmutableBytesWritable okey_md5 = new ImmutableBytesWritable(
      KeyUtils.makeEntryKey(key, ktype, md5_col)
    );
    final KeyValue kv_md5 =
      new KeyValue(key, family, md5_col, timestamp, md5);

    final ImmutableBytesWritable okey_size = new ImmutableBytesWritable(
      KeyUtils.makeEntryKey(key, ktype, size_col)
    );
    final KeyValue kv_size =
      new KeyValue(key, family, size_col, timestamp, Bytes.toBytes(hd.size));    
    final ImmutableBytesWritable okey_nsrl = new ImmutableBytesWritable(
      KeyUtils.makeEntryKey(key, ktype, nsrl_col)
    );
    final KeyValue kv_nsrl =
      new KeyValue(key, family, nsrl_col, timestamp, nsrl_col);

    final byte[] prod_col = 
      (md.name + '/' + pd.name + ' ' + pd.version).getBytes();
    final ImmutableBytesWritable okey_prod = new ImmutableBytesWritable(
      KeyUtils.makeEntryKey(key, ktype, prod_col)
    );
    final KeyValue kv_prod =
      new KeyValue(key, family, prod_col, timestamp, prod_col);

    final Map<Integer,List<ProdData>> prod =
      Collections.singletonMap(pd.code, Collections.singletonList(pd));
    final Map<String,MfgData> mfg = Collections.singletonMap(md.code, md);
    final Map<String,OSData> os = null;

    final HashLoaderMapper.Context ctx =
      context.mock(HashLoaderMapper.Context.class);
    
    context.checking(new Expectations() {
      {
        oneOf(ctx).write(okey_crc32, kv_crc32);

        if (sha1rowkey) {
          oneOf(ctx).write(okey_md5,  kv_md5);
        }
        else {
          oneOf(ctx).write(okey_sha1,  kv_sha1);
        }

        oneOf(ctx).write(okey_size,  kv_size);
        oneOf(ctx).write(okey_nsrl,  kv_nsrl);
        oneOf(ctx).write(okey_prod,  kv_prod);
      }
    });

    final HashLoaderHelper hlh = new HashLoaderHelper(prod, mfg, os, timestamp);
    hlh.writeRow(key, hd, ctx);
  }
 
  @Test
  public void writeRowMD5() throws Exception {
    writeRowTester(false);
  }

  @Test
  public void writeRowSHA1() throws Exception {
    writeRowTester(true);
  }
}
