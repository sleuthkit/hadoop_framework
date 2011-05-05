package com.lightboxtechnologies.nsrl;

/**
 * A {@link RecordProcessor} for NSRL manufacturer records.
 * 
 * @author Joel Uckelman
 */
class MfgRecordProcessor implements RecordProcessor {
  private final RecordConsumer<MfgData> consumer;

  public MfgRecordProcessor(RecordConsumer<MfgData> consumer) {
    this.consumer = consumer;
  }

  public void process(String[] col) throws BadDataException {
    if (col.length < 2) {
      throw new BadDataException("too few columns");
    }
    else if (col.length > 2) {
      throw new BadDataException("too many columns");
    }

    final MfgData md = new MfgData(col[0], col[1]);

    consumer.consume(md); 
  }
}
