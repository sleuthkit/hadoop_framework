package com.lightboxtechnologies.nsrl;

/**
 * A {@link RecordProcessor} for NSRL product records.
 *
 * @author Joel Uckelman
 */
class ProdRecordProcessor implements RecordProcessor<ProdData> {
  public ProdData process(String[] col) throws BadDataException {
    if (col.length < 7) {
      throw new BadDataException("too few columns");
    }
    else if (col.length > 7) {
      throw new BadDataException("too many columns");
    }

    Integer code = null;
    try {
      code = Integer.valueOf(col[0]);
    }
    catch (NumberFormatException e) {
      throw new BadDataException(e);
    }

    if (code < 0) {
      throw new BadDataException("code < 0");
    }

    return new ProdData(code, col[1], col[2], col[3], col[4], col[5], col[6]);
  }
}
