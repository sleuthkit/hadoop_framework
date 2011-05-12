package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Arrays;

/**
 * @author Joel Uckelman
 */
public class ProdRecordProcessorTest {
  private static final int code = 1;
  private static final String name = "Greatest Common Divisor Finder";
  private static final String version = "1.0";
  private static final String os_code = "42";
  private static final String mfg_code = "Euclid";
  private static final String language = "Ancient Greek";
  private static final String app_type = "Mathematical";

  @Test(expected=BadDataException.class)
  public void processTooFewCols() throws BadDataException {
    final RecordProcessor<ProdData> proc = new ProdRecordProcessor();
    proc.process(new String[] { "foo" });
  }

  @Test(expected=BadDataException.class)
  public void processTooManyCols() throws BadDataException {
    final RecordProcessor<ProdData> proc = new ProdRecordProcessor();
    final String[] cols = new String[9];
    Arrays.fill(cols, "foo");
    proc.process(cols);
  }

  @Test
  public void processJustRightCols() throws BadDataException {
    final ProdData pd =
      new ProdData(code, name, version, os_code, mfg_code, language, app_type);

    final RecordProcessor<ProdData> proc = new ProdRecordProcessor();
    assertEquals(pd, proc.process(new String[] { 
      String.valueOf(code), name, version, os_code,
      mfg_code, language, app_type 
    }));
  }

  @Test(expected=BadDataException.class)
  public void processNonnumericCode() throws BadDataException {
    final RecordProcessor<ProdData> proc = new ProdRecordProcessor();
    proc.process(new String[] { "not a number", "", "", "", "", "", "" });
  }

  @Test(expected=BadDataException.class)
  public void processNegativeCode() throws BadDataException {
    final RecordProcessor<ProdData> proc = new ProdRecordProcessor();
    proc.process(new String[] { "-1", "", "", "", "", "", "" });
  }
}
