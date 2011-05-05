package com.lightboxtechnologies.nsrl;

/**
 * A {@link RecordProcessor} for NSRL operating system records.
 * 
 * @author Joel Uckelman
 */
class OSRecordProcessor implements RecordProcessor {
  private final RecordConsumer<OSData> consumer;

  public OSRecordProcessor(RecordConsumer<OSData> consumer) {
    this.consumer = consumer;
  }

  public void process(String[] col) throws BadDataException {
    if (col.length < 4) {
      throw new BadDataException("too few columns");
    }
    else if (col.length > 4) {
      throw new BadDataException("too many columns");
    }

    final OSData osd = new OSData(col[0], col[1], col[2], col[3]);

    consumer.consume(osd);
  }
}
