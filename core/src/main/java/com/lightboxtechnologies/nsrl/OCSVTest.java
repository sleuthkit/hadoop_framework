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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

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

        final String sha1 = Hex.encodeHexString(hd.sha1);
        final String md5 = Hex.encodeHexString(hd.md5);
        final String crc32 = Hex.encodeHexString(hd.crc32);

        final JSONObject hd_json = new JSONObject();
        hd_json.put("sha1", sha1);
        hd_json.put("md5", md5);
        hd_json.put("crc32", crc32);
        hd_json.put("name", hd.name);
        hd_json.put("size", hd.size);
        hd_json.put("special_code", hd.special_code);
        
        final OSData osd = os.get(hd.os_code);
        final MfgData osmfgd = mfg.get(osd.mfg_code);

        final JSONObject os_json = new JSONObject();
        os_json.put("name", osd.name);
        os_json.put("version", osd.version);
        os_json.put("manufacturer", osmfgd.name);
        hd_json.put("os", os_json);

        final JSONArray pl_json = new JSONArray();
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

          final JSONObject prod_json = new JSONObject();
          
          prod_json.put("name", pd.name);
          prod_json.put("version", pd.version);
          prod_json.put("language", pd.language);
          prod_json.put("app_type", pd.app_type);
          prod_json.put("os_code", pd.os_code);

          final MfgData md = mfg.get(pd.mfg_code);
          prod_json.put("manufacturer", md.name);

          pl_json.add(prod_json);
        }

if (pl_json.size() > 1) {
  System.err.println(hd.prod_code);
}

        hd_json.put("products", pl_json);

        System.out.println(hd_json.toString());
      }
    };

    final RecordProcessor<HashData> hproc = new HashRecordProcessor();
    final LineHandler hlh =
      new DefaultLineHandler<HashData>(tok, hproc, hcon, err);

    InputStream zin = null;
    try {
      zin = new GZIPInputStream(new FileInputStream(hash_filename));
//      zin = new FileInputStream(hash_filename);
      loader.load(zin, hlh);
      zin.close();
    }
    finally {
      IOUtils.closeQuietly(zin);
    }

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
