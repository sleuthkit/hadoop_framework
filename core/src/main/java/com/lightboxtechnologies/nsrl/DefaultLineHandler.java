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

import java.io.IOException;

/**
 * A basic NSRL record line handling class.
 *
 * @author Joel Uckelman
 */
public class DefaultLineHandler<T> implements LineHandler {

  private final LineTokenizer tok;
  private final RecordProcessor<T> proc;
  private final RecordConsumer<T> con;
  private final ErrorConsumer err;

  public DefaultLineHandler(LineTokenizer tok, RecordProcessor<T> proc,
                            RecordConsumer<T> con, ErrorConsumer err) {
    if (tok == null) throw new IllegalArgumentException();
    if (proc == null) throw new IllegalArgumentException();
    if (err == null) throw new IllegalArgumentException();

    this.tok = tok;
    this.proc = proc;
    this.con = con;
    this.err = err;
  }

  public void handle(String line, long linenum) throws IOException {
    final String[] cols = tok.tokenize(line);

    try {
      final T dat = proc.process(cols);
      con.consume(dat);
    }
    catch (BadDataException e) {
      err.consume(e, linenum);
    }
  }
}
