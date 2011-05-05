package com.lightboxtechnologies.nsrl;

/**
 * An interface for consumers of NSRL records.
 *
 * @author Joel Uckelman
 */
public interface RecordConsumer<T> {
  public void consume(T record);
}
