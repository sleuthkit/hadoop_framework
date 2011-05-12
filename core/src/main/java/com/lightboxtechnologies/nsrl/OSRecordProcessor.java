package com.lightboxtechnologies.nsrl;

/**
 * A {@link RecordProcessor} for NSRL operating system records.
 *
 * @author Joel Uckelman
 */
class OSRecordProcessor implements RecordProcessor<OSData> {
  public OSData process(String[] col) throws BadDataException {
    if (col.length < 4) {
      throw new BadDataException("too few columns");
    }
    else if (col.length > 4) {
      throw new BadDataException("too many columns");
    }

    return new OSData(col[0], col[1], col[2], col[3]);
  }
}
