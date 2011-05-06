package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;

import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;

import org.apache.commons.io.input.CharSequenceReader;
import org.apache.commons.lang.StringUtils;

import com.lightboxtechnologies.io.IOUtils;


/**
 * @author Joel Uckelman
 */
@RunWith(JMock.class)
public class OpenCSVRecordReaderTest {
  private final Mockery context = new JUnit4Mockery();

  private static final String[][] cols = {
    new String[] { "skip", "skip",    "skip" },
    new String[] { "foo",  "bar,baz", "1"    },
    new String[] { "xxx",  "yyy",     "2"    },
    new String[] { "aaa",  "bbb",     "3"    }
  };

  protected RecordProcessor buildRecordProcessorGood()
                                                      throws BadDataException {
    final RecordProcessor proc = context.mock(RecordProcessor.class);

    context.checking(new Expectations() {
      {
        // skip the first line, it defines column names
        never(proc).process(with(cols[0]));

        // process all remaining lines
        for (int i = 1; i < cols.length; ++i) {
          oneOf(proc).process(with(cols[i]));
        }
      }
    });

    return proc;
  }

  protected ErrorConsumer buildErrorConsumerGood() {
    final ErrorConsumer err = context.mock(ErrorConsumer.class);

    context.checking(new Expectations() {
      {
        never(err).consume(with(any(BadDataException.class)),
                           with(any(long.class)));
      }
    });

    return err;
  }

  private static final String bde_msg = "No! Bad data! BAD!";

  protected RecordProcessor buildRecordProcessorBad() throws BadDataException {
    final RecordProcessor proc = context.mock(RecordProcessor.class);

    context.checking(new Expectations() {
      {
        // skip the first line, it defines column names
        never(proc).process(with(cols[0]));

        // process line 2
        oneOf(proc).process(with(cols[1]));

        // line 3 is bad
        oneOf(proc).process(with(cols[2]));
        will(throwException(new BadDataException(bde_msg)));

        // process line 4
        oneOf(proc).process(with(cols[3]));
      }
    });

    return proc;
  }

  protected ErrorConsumer buildErrorConsumerBad() {
    final ErrorConsumer err = context.mock(ErrorConsumer.class);

    context.checking(new Expectations() {
      {
        // line 2 is bad
        oneOf(err).consume(with(any(BadDataException.class)), with(equal(2L)));
      }
    });

    return err;
  }

  @Test
  public void readFromReaderGood() throws IOException, BadDataException {
    final RecordProcessor proc = buildRecordProcessorGood();
    final RecordReader rr = new OpenCSVRecordReader(buildErrorConsumerGood());

    Reader r = null;
    try {
      r = new CharSequenceReader(buildCSV(cols));
      rr.read(r, proc);
      r.close();
    }
    finally {
      IOUtils.closeQuietly(r);
    }
  }

  @Test
  public void readFromReaderBad() throws IOException, BadDataException {
    final RecordProcessor proc = buildRecordProcessorBad();
    final RecordReader rr = new OpenCSVRecordReader(buildErrorConsumerBad());

    Reader r = null;
    try {
      r = new CharSequenceReader(buildCSV(cols));
      rr.read(r, proc);
      r.close();
    }
    finally {
      IOUtils.closeQuietly(r);
    }
  }

  @Test
  public void readFromInputStreamGood() throws IOException, BadDataException {
    final RecordProcessor proc = buildRecordProcessorGood();
    final RecordReader rr = new OpenCSVRecordReader(buildErrorConsumerGood());

    InputStream in = null;
    try {
      in = IOUtils.toInputStream(buildCSV(cols));
      rr.read(in, proc);
      in.close();
    }
    finally {
      IOUtils.closeQuietly(in);
    }
  }

  @Test
  public void readFromInputStreamBad() throws IOException, BadDataException {
    final RecordProcessor proc = buildRecordProcessorBad();
    final RecordReader rr = new OpenCSVRecordReader(buildErrorConsumerBad());

    InputStream in = null;
    try {
      in = IOUtils.toInputStream(buildCSV(cols));
      rr.read(in, proc);
      in.close();
    }
    finally {
      IOUtils.closeQuietly(in);
    }
  }

  @Test
  public void readFromFileGood() throws IOException, BadDataException {
    final RecordProcessor proc = buildRecordProcessorGood();
    final RecordReader rr = new OpenCSVRecordReader(buildErrorConsumerGood());

    rr.read("src/test/java/com/lightboxtechnologies/nsrl/test.csv", proc);
  }

  @Test
  public void readFromFileBad() throws IOException, BadDataException {
    final RecordProcessor proc = buildRecordProcessorBad();
    final RecordReader rr = new OpenCSVRecordReader(buildErrorConsumerBad());

    rr.read("src/test/java/com/lightboxtechnologies/nsrl/test.csv", proc);
  }

  protected String buildCSV(String[][] cols) {
    final String[] csv = new String[cols.length];
    for (int i = 0; i < cols.length; ++i) {
      csv[i] = String.format("\"%s\",\"%s\",%s\n", (Object[]) cols[i]);
    }
    return StringUtils.join(csv);
  }
}
