/*
Copyright 2011, Lightbox Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;

import java.io.IOException;

/**
 * @author Joel Uckelman
 */
@RunWith(JMock.class)
public class DefaultLineHandlerTest {
  private final Mockery context = new JUnit4Mockery();

  private static final String[][] rows = {
    new String[] { "foo",  "bar,baz", "1"    },
    new String[] { "xxx",  "yyy",     "2"    },
    new String[] { "aaa",  "bbb",     "3"    }
  };

  @Test(expected=IllegalArgumentException.class)
  public void nullLineTokeniser() {
    @SuppressWarnings("unchecked")
    final RecordProcessor<Object> proc =
      (RecordProcessor<Object>) context.mock(RecordProcessor.class);
    @SuppressWarnings("unchecked")
    final RecordConsumer<Object> con =
      (RecordConsumer<Object>) context.mock(RecordConsumer.class);
    final ErrorConsumer err = context.mock(ErrorConsumer.class);
    new DefaultLineHandler<Object>(null, proc, con, err);
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullRecordProcessor() {
    final LineTokenizer tok = context.mock(LineTokenizer.class);
    @SuppressWarnings("unchecked")
    final RecordConsumer<Object> con =
      (RecordConsumer<Object>) context.mock(RecordConsumer.class);
    final ErrorConsumer err = context.mock(ErrorConsumer.class);
    new DefaultLineHandler<Object>(tok, null, con, err);
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullErrorConsumer() {
    final LineTokenizer tok = context.mock(LineTokenizer.class);
    @SuppressWarnings("unchecked")
    final RecordProcessor<Object> proc =
      (RecordProcessor<Object>) context.mock(RecordProcessor.class);
    @SuppressWarnings("unchecked")
    final RecordConsumer<Object> con =
      (RecordConsumer<Object>) context.mock(RecordConsumer.class);
    new DefaultLineHandler<Object>(tok, proc, con, null);
  }

  protected String buildCSV(String[] row) {
    return String.format("\"%s\",\"%s\",%s", (Object[]) row);
  }

  @Test
  public void handleGood() throws IOException, BadDataException {
    final LineTokenizer tok = context.mock(LineTokenizer.class);
    @SuppressWarnings("unchecked")
    final RecordProcessor<Object> proc =
      (RecordProcessor<Object>) context.mock(RecordProcessor.class);
    @SuppressWarnings("unchecked")
    final RecordConsumer<Object> con =
      (RecordConsumer<Object>) context.mock(RecordConsumer.class);
    final ErrorConsumer err = context.mock(ErrorConsumer.class);

    context.checking(new Expectations() {
      {
        for (int i = 0; i < rows.length; ++i) {
          oneOf(tok).tokenize(with(buildCSV(rows[i])));
          will(returnValue(rows[i]));

          oneOf(proc).process(with(rows[i]));
          will(returnValue("foo"));

          oneOf(con).consume(with("foo"));
        }

        never(err).consume(with(any(BadDataException.class)),
                           with(any(long.class)));
      }
    });

    final LineHandler lh = new DefaultLineHandler<Object>(tok, proc, con, err);
    for (int i = 0; i < rows.length; ++i) {
      lh.handle(buildCSV(rows[i]), i+1);
    }
  }

  @Test
  public void handleBad() throws IOException, BadDataException {
    final LineTokenizer tok = context.mock(LineTokenizer.class);
    @SuppressWarnings("unchecked")
    final RecordProcessor<Object> proc =
      (RecordProcessor<Object>) context.mock(RecordProcessor.class);
    @SuppressWarnings("unchecked")
    final RecordConsumer<Object> con =
      (RecordConsumer<Object>) context.mock(RecordConsumer.class);
    final ErrorConsumer err = context.mock(ErrorConsumer.class);

    context.checking(new Expectations() {
      {
        for (int i = 0; i < rows.length; ++i) {
          oneOf(tok).tokenize(with(buildCSV(rows[i])));
          will(returnValue(rows[i]));
        }

        oneOf(proc).process(with(rows[0]));
        will(returnValue("foo"));

        oneOf(con).consume(with("foo"));

        // line 2 is "bad"
        oneOf(proc).process(with(rows[1]));
        will(throwException(new BadDataException()));

        oneOf(err).consume(with(any(BadDataException.class)), with(2L));

        oneOf(proc).process(with(rows[2]));
        will(returnValue("foo"));

        oneOf(con).consume(with("foo"));
      }
    });

    final LineHandler lh = new DefaultLineHandler<Object>(tok, proc, con, err);
    for (int i = 0; i < rows.length; ++i) {
      lh.handle(buildCSV(rows[i]), i+1);
    }
  }
}
