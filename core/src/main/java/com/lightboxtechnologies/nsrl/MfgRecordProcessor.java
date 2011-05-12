package com.lightboxtechnologies.nsrl;

/**
 * A {@link RecordProcessor} for NSRL manufacturer records.
 *
 * @author Joel Uckelman
 */
class MfgRecordProcessor implements RecordProcessor<MfgData> {
  public MfgData process(String[] col) throws BadDataException {
    if (col.length < 2) {
      throw new BadDataException("too few columns");
    }
    else if (col.length > 2) {
      throw new BadDataException("too many columns");
    }

    return new MfgData(col[0], col[1]);
  }
}
