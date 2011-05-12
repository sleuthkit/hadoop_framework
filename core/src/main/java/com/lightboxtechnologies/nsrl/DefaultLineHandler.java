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
