package com.lightboxtechnologies.nsrl;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import com.lightboxtechnologies.io.IOUtils;

public class OCSVTest {
  protected OCSVTest() {}

  public static void main(String[] args) throws IOException {
    final String mfg_filename  = args[0];
    final String os_filename   = args[1];
    final String prod_filename = args[2];
    final String hash_filename = args[3];

    final RecordReader reader = new OpenCSVRecordReader();

    // load manufacturer data
    final Map<String,String> mfg = new HashMap<String,String>();

    final RecordConsumer<MfgData> mfgcon = new RecordConsumer<MfgData>() {
      public void consume(MfgData md) {
        mfg.put(md.code, md.name);
      }
    };

    reader.read(mfg_filename, new MfgRecordProcessor(mfgcon));

    // load OS data
    final Map<String,OSData> os = new HashMap<String,OSData>();

    final RecordConsumer<OSData> oscon = new RecordConsumer<OSData>() {
      public void consume(OSData osd) {
        os.put(osd.code, osd);
      }
    };

    reader.read(os_filename, new OSRecordProcessor(oscon));

    // load product data
    final Map<Integer,List<ProdData>> prod =
      new HashMap<Integer,List<ProdData>>();

    final RecordConsumer<ProdData> prodcon = new RecordConsumer<ProdData>() {
      public void consume(ProdData pd) {
        List<ProdData> plist = prod.get(pd.code);
        if (plist == null) {
          plist = new ArrayList<ProdData>();
          prod.put(pd.code, plist);
        }

        plist.add(pd);
      }
    };

    reader.read(prod_filename, new ProdRecordProcessor(prodcon));

/*
    // read hash data
    InputStream zin = null;
    try {
      zin = new GZIPInputStream(new FileInputStream(hash_filename));
      reader.read(zin, new HashRecordProcessor());
      zin.close();
    }
    finally {
      IOUtils.closeQuietly(zin);
    }
*/

    for (Map.Entry<String,String> e : mfg.entrySet()) {
      System.out.println(e.getKey() + " = " + e.getValue());
    }

    for (Map.Entry<String,OSData> e : os.entrySet()) {
      System.out.println(e.getKey() + " = " + e.getValue());
    }

    for (Map.Entry<Integer,List<ProdData>> e : prod.entrySet()) {
      System.out.println(e.getKey() + " = " + e.getValue());
    }
  }
}
