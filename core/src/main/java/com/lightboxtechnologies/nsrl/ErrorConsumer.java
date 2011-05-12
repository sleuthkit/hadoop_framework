package com.lightboxtechnologies.nsrl;

/**
 * An interface for consumers of NSRL record errors.
 *
 * @author Joel Uckelman
 */
public interface ErrorConsumer {
  public void consume(BadDataException bde, long linenum);
}
