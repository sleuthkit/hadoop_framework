package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Joel Uckelman
 */
public class MfgRecordProcessorTest {
  @Test(expected=BadDataException.class)
  public void processTooFewCols() throws BadDataException {
    final RecordProcessor<MfgData> proc = new MfgRecordProcessor();
    proc.process(new String[] { "foo" });
  }

  @Test(expected=BadDataException.class)
  public void processTooManyCols() throws BadDataException {
    final RecordProcessor<MfgData> proc = new MfgRecordProcessor();
    proc.process(new String[] { "foo", "foo", "foo" });
  }

  @Test
  public void processJustRightCols() throws BadDataException {
    final String code = "SPFT";
    final String name = "Stay Puft Marshmallow Corporation";

    final MfgData md = new MfgData(code, name);

    final RecordProcessor<MfgData> proc = new MfgRecordProcessor();
    assertEquals(md, proc.process(new String[] { md.code, md.name }));
  }
}
