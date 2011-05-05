package com.lightboxtechnologies.nsrl;

/**
 * An {@link Exception} indicating malformed data.
 *
 * @author Joel Uckelman
 */
public class BadDataException extends Exception {
  private static final long serialVersionUID = 1L;

  public BadDataException() {}

  public BadDataException(String msg) { super(msg); }

  public BadDataException(Throwable cause) { super(cause); }

  public BadDataException(String msg, Throwable cause) { super(msg, cause); }
}
