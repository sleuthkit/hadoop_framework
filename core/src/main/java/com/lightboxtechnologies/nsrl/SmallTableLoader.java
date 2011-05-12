package com.lightboxtechnologies.nsrl;

import java.io.FileReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.commons.io.IOUtils;

/**
 * Utility method for loading manufacturer, OS, and product data.
 *
 * @author Joel Uckelman
 */
public class SmallTableLoader {

  protected SmallTableLoader() {}

  protected static void load(FileSystem fs, String filename,
                             LineHandler lh, RecordLoader loader)
                                                           throws IOException {
    InputStream in = null;
    try {
      in = fs.open(new Path(filename));
      loader.load(in, lh);
      in.close();
    }
    finally {
      IOUtils.closeQuietly(in);
    }
  }

  protected static void load(String filename,
                             LineHandler lh, RecordLoader loader)
                                                           throws IOException {
    Reader r = null;
    try {
      r = new FileReader(filename);
      loader.load(r, lh);
      r.close();
    }
    finally {
      IOUtils.closeQuietly(r);
    }
  }

  protected static LineHandler getMfgLH(LineTokenizer tok, ErrorConsumer err,
                                        final Map<String,MfgData> mfg)
                                                           throws IOException {

    // load manufacturer data
    final RecordConsumer<MfgData> mfg_con = new RecordConsumer<MfgData>() {
      public void consume(MfgData md) {
        mfg.put(md.code, md);
      }
    };
    
    final RecordProcessor<MfgData> mfg_proc = new MfgRecordProcessor();
    return new DefaultLineHandler<MfgData>(tok, mfg_proc, mfg_con, err);
  }

  protected static LineHandler getOSLH(LineTokenizer tok, ErrorConsumer err,
                                       final Map<String,OSData> os)
                                                           throws IOException {
    
    // load OS data
    final RecordConsumer<OSData> os_con = new RecordConsumer<OSData>() {
      public void consume(OSData osd) {
        os.put(osd.code, osd);
      }
    };

    final RecordProcessor<OSData> os_proc = new OSRecordProcessor();
    return new DefaultLineHandler<OSData>(tok, os_proc, os_con, err);
  }  

  protected static LineHandler getProdLH(LineTokenizer tok, ErrorConsumer err,
                                         final Map<Integer,List<ProdData>> prod)
                                                           throws IOException {

    // load product data
    final RecordConsumer<ProdData> prod_con = new RecordConsumer<ProdData>() {
      public void consume(ProdData pd) {
        List<ProdData> plist = prod.get(pd.code);
        if (plist == null) {
          plist = new ArrayList<ProdData>();
          prod.put(pd.code, plist);
        }

        plist.add(pd);
      }
    };

    final RecordProcessor<ProdData> prod_proc = new ProdRecordProcessor();
    return new DefaultLineHandler<ProdData>(tok, prod_proc, prod_con, err);
  }

  public static void load(
    InputStream mfg_in,  final Map<String,MfgData> mfg,
    InputStream os_in,   final Map<String,OSData> os,
    InputStream prod_in, final Map<Integer,List<ProdData>> prod,
    LineTokenizer tok,   ErrorConsumer err)
                                                           throws IOException {
    final RecordLoader loader = new RecordLoader();
    loader.load(mfg_in, getMfgLH(tok, err, mfg));
    loader.load(os_in, getOSLH(tok, err, os));
    loader.load(prod_in, getProdLH(tok, err, prod));
  }

  public static void load(
    FileSystem fs,
    String mfg_filename,  final Map<String,MfgData> mfg,
    String os_filename,   final Map<String,OSData> os,
    String prod_filename, final Map<Integer,List<ProdData>> prod,
    LineTokenizer tok,    ErrorConsumer err)
                                                           throws IOException {

    final RecordLoader loader = new RecordLoader();
    load(fs, mfg_filename, getMfgLH(tok, err, mfg), loader);
    load(fs, os_filename, getOSLH(tok, err, os), loader);
    load(fs, prod_filename, getProdLH(tok, err, prod), loader);
  }

  public static void load(
    String mfg_filename,  final Map<String,MfgData> mfg,
    String os_filename,   final Map<String,OSData> os,
    String prod_filename, final Map<Integer,List<ProdData>> prod,
    LineTokenizer tok,    ErrorConsumer err)
                                                           throws IOException {

    final RecordLoader loader = new RecordLoader();
    load(mfg_filename, getMfgLH(tok, err, mfg), loader);
    load(os_filename, getOSLH(tok, err, os), loader);
    load(prod_filename, getProdLH(tok, err, prod), loader);
  }
}
