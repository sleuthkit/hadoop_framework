package com.lightboxtechnologies.nsrl;

/**
 * The interface for processors of NSRL records.
 * 
 * @author Joel Uckelman
 */
public interface RecordProcessor {
  public void process(String[] col) throws BadDataException;
}
