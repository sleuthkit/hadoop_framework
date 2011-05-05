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
public class MfgRecordProcessorTest {
  private final Mockery context = new JUnit4Mockery(); 
 
  private static final RecordConsumer<MfgData> dummy =
                                                new RecordConsumer<MfgData>() {
    public void consume(MfgData md) {}
  };
  
  @Test(expected=BadDataException.class)
  public void processTooFewCols() throws BadDataException {
    final RecordProcessor proc = new MfgRecordProcessor(dummy);
    proc.process(new String[] { "foo" });
  }

  @Test(expected=BadDataException.class)
  public void processTooManyCols() throws BadDataException {
    final RecordProcessor proc = new MfgRecordProcessor(dummy);
    proc.process(new String[] { "foo", "foo", "foo" });
  }

  @Test
  public void processJustRightCols() throws BadDataException {
    final String code = "SPFT";
    final String name = "Stay Puft Marshmallow Corporation";

    final MfgData md = new MfgData(code, name); 

    @SuppressWarnings("unchecked")
    final RecordConsumer<MfgData> mfgcon =
      (RecordConsumer<MfgData>) context.mock(RecordConsumer.class);

    context.checking(new Expectations() {
      {
        oneOf(mfgcon).consume(with(md));
      }
    });

    final RecordProcessor proc = new MfgRecordProcessor(mfgcon);
    proc.process(new String[] { code, name });
  }
}
