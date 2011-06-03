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

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.binary.Hex;

import org.codehaus.jackson.map.ObjectMapper;

import com.lightboxtechnologies.io.IOUtils;

public class OCSVTest {
  protected OCSVTest() {}

  public static void main(String[] args) throws IOException {
    final String mfg_filename  = args[0];
    final String os_filename   = args[1];
    final String prod_filename = args[2];
    final String hash_filename = args[3];

    final LineTokenizer tok = new OpenCSVLineTokenizer();
    final RecordLoader loader = new RecordLoader();

    final ErrorConsumer err = new ErrorConsumer() {
      public void consume(BadDataException e, long linenum) {
        System.err.println("malformed record, line " + linenum);
        e.printStackTrace();
      }
    };

    // read manufacturer, OS, product data
    final Map<String,MfgData> mfg = new HashMap<String,MfgData>();
    final Map<String,OSData> os = new HashMap<String,OSData>();
    final Map<Integer,List<ProdData>> prod =
      new HashMap<Integer,List<ProdData>>();

    SmallTableLoader.load(
      mfg_filename, mfg, os_filename, os, prod_filename, prod, tok, err
    );

    // read hash data
    final RecordConsumer<HashData> hcon = new RecordConsumer<HashData>() {
      public void consume(HashData hd) {
/*
        System.out.print(hd);

        for (ProdData pd : prod.get(hd.prod_code)) {
          System.out.print(pd);
          final MfgData pmd = mfg.get(pd.mfg_code);
          System.out.print(pmd);
        }

        final OSData osd = os.get(hd.os_code);
        System.out.print(osd);

        final MfgData osmd = mfg.get(osd.mfg_code);
        System.out.print(osmd);

        System.out.println("");
*/

/*
        final Hex hex = new Hex();

        String sha1 = null;
        String md5 = null;
        String crc32 = null;

        try {
          sha1 = (String) hex.encode((Object) hd.sha1);
          md5 = (String) hex.encode((Object) hd.md5);
          crc32 = (String) hex.encode((Object) hd.crc32);
        }
        catch (EncoderException e) {
          throw new RuntimeException(e);
        }
*/

        final ObjectMapper mapper = new ObjectMapper();

        final String sha1 = Hex.encodeHexString(hd.sha1);
        final String md5 = Hex.encodeHexString(hd.md5);
        final String crc32 = Hex.encodeHexString(hd.crc32);

        final Map<String,Object> hd_m = new HashMap<String,Object>();

        hd_m.put("sha1", sha1);
        hd_m.put("md5", md5);
        hd_m.put("crc32", crc32);
        hd_m.put("name", hd.name);
        hd_m.put("size", hd.size);
        hd_m.put("special_code", hd.special_code);

        final OSData osd = os.get(hd.os_code);
        final MfgData osmfgd = mfg.get(osd.mfg_code);

        final Map<String,Object> os_m = new HashMap<String,Object>();
        os_m.put("name", osd.name);
        os_m.put("version", osd.version);
        os_m.put("manufacturer", osmfgd.name);
        hd_m.put("os", os_m);

        final List<Map<String,Object>> pl_l =
          new ArrayList<Map<String,Object>>();
        for (ProdData pd : prod.get(hd.prod_code)) {

          if (!osd.code.equals(pd.os_code)) {
            // os code mismatch
/*
            System.err.println(
              "Hash record OS code == " + osd.code + " != " + pd.os_code + " == product record OS code"
            );
*/
            continue;
          }

          final Map<String,Object> prod_m = new HashMap<String,Object>();

          prod_m.put("name", pd.name);
          prod_m.put("version", pd.version);
          prod_m.put("language", pd.language);
          prod_m.put("app_type", pd.app_type);
          prod_m.put("os_code", pd.os_code);

          final MfgData md = mfg.get(pd.mfg_code);
          prod_m.put("manufacturer", md.name);

          pl_l.add(prod_m);
        }

        if (pl_l.size() > 1) {
          System.err.println(hd.prod_code);
        }

        hd_m.put("products", pl_l);

        try {
          mapper.writeValue(System.out, hd_m);
        }
        catch (IOException e) {
          // should be impossible
          throw new IllegalStateException(e);
        }
      }
    };

    final RecordProcessor<HashData> hproc = new HashRecordProcessor();
    final LineHandler hlh =
      new DefaultLineHandler<HashData>(tok, hproc, hcon, err);

    InputStream zin = null;
    try {
//      zin = new GZIPInputStream(new FileInputStream(hash_filename));
      zin = new FileInputStream(hash_filename);
      loader.load(zin, hlh);
      zin.close();
    }
    finally {
      IOUtils.closeQuietly(zin);
    }

    System.out.println();

/*
    for (Map.Entry<String,String> e : mfg.entrySet()) {
      System.out.println(e.getKey() + " = " + e.getValue());
    }

    for (Map.Entry<String,OSData> e : os.entrySet()) {
      System.out.println(e.getKey() + " = " + e.getValue());
    }

    for (Map.Entry<Integer,List<ProdData>> e : prod.entrySet()) {
      System.out.println(e.getKey() + " = " + e.getValue());
    }
*/
  }
}
