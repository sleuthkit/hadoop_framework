package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;

import java.util.Arrays;

/**
 * @author Joel Uckelman
 */
@RunWith(JMock.class)
public class ProdRecordProcessorTest {
  private final Mockery context = new JUnit4Mockery();

  private static final RecordConsumer<ProdData> dummy =
                                               new RecordConsumer<ProdData>() {
    public void consume(ProdData osd) {}
  };

  private static final int code = 1;
  private static final String name = "Greatest Common Divisor Finder";
  private static final String version = "1.0";
  private static final String os_code = "42";
  private static final String mfg_code = "Euclid";
  private static final String language = "Ancient Greek";
  private static final String app_type = "Mathematical";

  @Test(expected=BadDataException.class)
  public void processTooFewCols() throws BadDataException {
    final RecordProcessor proc = new ProdRecordProcessor(dummy);
    proc.process(new String[] { "foo" });
  }

  @Test(expected=BadDataException.class)
  public void processTooManyCols() throws BadDataException {
    final RecordProcessor proc = new ProdRecordProcessor(dummy);
    final String[] cols = new String[9];
    Arrays.fill(cols, "foo");
    proc.process(cols);
  }

  @Test
  public void processJustRightCols() throws BadDataException {
    final ProdData pd =
      new ProdData(code ,name, version, os_code, mfg_code, language, app_type);

    @SuppressWarnings("unchecked")
    final RecordConsumer<ProdData> prodcon =
      (RecordConsumer<ProdData>) context.mock(RecordConsumer.class);

    context.checking(new Expectations() {
      {
        oneOf(prodcon).consume(with(pd));
      }
    });

    final RecordProcessor proc = new ProdRecordProcessor(prodcon);
    proc.process(new String[] { 
      String.valueOf(code), name, version, os_code,
      mfg_code, language, app_type 
    });
  }

  @Test(expected=BadDataException.class)
  public void processNonnumericCode() throws BadDataException {
    final RecordProcessor proc = new ProdRecordProcessor(dummy);
    proc.process(new String[] { "not a number", "", "", "", "", "", "" });
  }

  @Test(expected=BadDataException.class)
  public void processNegativeCode() throws BadDataException {
    final RecordProcessor proc = new ProdRecordProcessor(dummy);
    proc.process(new String[] { "-1", "", "", "", "", "", "" });
  }

  @Test
  public void processSeveralRecordsSameKey() throws BadDataException {
    final ProdData pd =
      new ProdData(code ,name, version, os_code, mfg_code, language, app_type);

    final int limit = 10;

    @SuppressWarnings("unchecked")
    final RecordConsumer<ProdData> prodcon =
      (RecordConsumer<ProdData>) context.mock(RecordConsumer.class);

    context.checking(new Expectations() {
      {
        exactly(limit).of(prodcon).consume(with(pd));
      }
    });

    final RecordProcessor proc = new ProdRecordProcessor(prodcon);

    for (int i = 0; i < limit; ++i) {
      proc.process(new String[] { 
        String.valueOf(code), name, version, os_code,
        mfg_code, language, app_type 
      });
    }
  }
}
