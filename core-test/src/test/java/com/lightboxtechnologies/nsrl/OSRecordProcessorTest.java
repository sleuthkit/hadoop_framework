package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;

/**
 * @author Joel Uckelman
 */
@RunWith(JMock.class)
public class OSRecordProcessorTest {
  private final Mockery context = new JUnit4Mockery();

  private static final RecordConsumer<OSData> dummy =
                                                 new RecordConsumer<OSData>() {
    public void consume(OSData osd) {}
  };

  @Test(expected=BadDataException.class)
  public void processTooFewCols() throws BadDataException {
    final RecordProcessor proc = new OSRecordProcessor(dummy);
    proc.process(new String[] { "foo" });
  }

  @Test(expected=BadDataException.class)
  public void processTooManyCols() throws BadDataException {
    final RecordProcessor proc = new OSRecordProcessor(dummy);
    proc.process(new String[] { "foo", "foo", "foo", "foo", "foo" });
  }

  @Test
  public void processJustRightCols() throws BadDataException {
    final String code = "1";  
    final String name = "TuringOS";
    final String version = "0.1";
    final String mfg_code = "AT";
 
    final OSData osd = new OSData(code, name, version, mfg_code);

    @SuppressWarnings("unchecked")
    final RecordConsumer<OSData> oscon =
      (RecordConsumer<OSData>) context.mock(RecordConsumer.class);

    context.checking(new Expectations() {
      {
        oneOf(oscon).consume(with(osd));
      }
    });

    final RecordProcessor proc = new OSRecordProcessor(oscon);
    proc.process(
      new String[] { osd.code, osd.name, osd.version, osd.mfg_code }
    );
  }
}
