package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Joel Uckelman
 */
public class OSRecordProcessorTest {
  @Test(expected=BadDataException.class)
  public void processTooFewCols() throws BadDataException {
    final RecordProcessor<OSData> proc = new OSRecordProcessor();
    proc.process(new String[] { "foo" });
  }

  @Test(expected=BadDataException.class)
  public void processTooManyCols() throws BadDataException {
    final RecordProcessor<OSData> proc = new OSRecordProcessor();
    proc.process(new String[] { "foo", "foo", "foo", "foo", "foo" });
  }

  @Test
  public void processJustRightCols() throws BadDataException {
    final String code = "1";
    final String name = "TuringOS";
    final String version = "0.1";
    final String mfg_code = "AT";

    final OSData osd = new OSData(code, name, version, mfg_code);

    final RecordProcessor proc = new OSRecordProcessor();
    assertEquals(osd, proc.process(
      new String[] { osd.code, osd.name, osd.version, osd.mfg_code }
    ));
  }
}
