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
