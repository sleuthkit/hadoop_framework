package com.lightboxtechnologies.nsrl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;

import java.util.Arrays;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * @author Joel Uckelman
 */
@RunWith(JMock.class)
public class HashRecordProcessorTest {
  private final Mockery context = new JUnit4Mockery();

  private static final RecordConsumer<HashData> dummy =
                                               new RecordConsumer<HashData>() {
    public void consume(HashData osd) {}
  };

  private static final Hex hex = new Hex();

  private static final String name = "c'est ne pas un name";
  private static final long size = 42;
  private static final int prod_code = 13;
  private static final String os_code = "89";
  private static final String special_code = "so fucking special";

  private static byte[] sha1;
  private static byte[] md5;
  private static byte[] crc32;

  @BeforeClass
  public static void init() throws DecoderException {
    sha1  = (byte[]) hex.decode("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    md5   = (byte[]) hex.decode("deadbeefdeadbeefdeadbeefdeadbeef");
    crc32 = (byte[]) hex.decode("deadbeef");
  }

  @Test(expected=BadDataException.class)
  public void processTooFewCols() throws BadDataException {
    final RecordProcessor proc = new HashRecordProcessor(dummy);
    proc.process(new String[] { "foo" });
  }

  @Test(expected=BadDataException.class)
  public void processTooManyCols() throws BadDataException {
    final RecordProcessor proc = new HashRecordProcessor(dummy);
    final String[] cols = new String[9];
    Arrays.fill(cols, "foo");
    proc.process(cols);
  }

  @Test(expected=BadDataException.class)
  public void processNegativeSize() throws BadDataException {
    final RecordProcessor proc = new HashRecordProcessor(dummy);
    proc.process(new String[] {
      Hex.encodeHexString(sha1), Hex.encodeHexString(md5),
      Hex.encodeHexString(crc32), name, String.valueOf(-1),
      String.valueOf(prod_code), os_code, special_code
    });
  }

  @Test(expected=BadDataException.class)
  public void processNonnumericSize() throws BadDataException {
    final RecordProcessor proc = new HashRecordProcessor(dummy);
    proc.process(new String[] {
      Hex.encodeHexString(sha1), Hex.encodeHexString(md5),
      Hex.encodeHexString(crc32), name, "foo",
      String.valueOf(prod_code), os_code, special_code
    });
  }

  @Test(expected=BadDataException.class)
  public void processNegativeProdCode() throws BadDataException {
    final RecordProcessor proc = new HashRecordProcessor(dummy);
    proc.process(new String[] {
      Hex.encodeHexString(sha1), Hex.encodeHexString(md5),
      Hex.encodeHexString(crc32), name, String.valueOf(size),
      "-1", os_code, special_code
    });
  }

  @Test(expected=BadDataException.class)
  public void processNonnumericProdCode() throws BadDataException {
    final RecordProcessor proc = new HashRecordProcessor(dummy);
    proc.process(new String[] {
      Hex.encodeHexString(sha1), Hex.encodeHexString(md5),
      Hex.encodeHexString(crc32), name, String.valueOf(size),
      "foo", os_code, special_code
    });
  }

  @Test
  public void processJustRightCols() throws BadDataException, DecoderException {
    final HashData hd = new HashData(
      sha1, md5, crc32, name, size, prod_code, os_code, special_code);

    @SuppressWarnings("unchecked")
    final RecordConsumer<HashData> hcon =
      (RecordConsumer<HashData>) context.mock(RecordConsumer.class);

    context.checking(new Expectations() {
      {
        oneOf(hcon).consume(with(hd));
      }
    });

    final RecordProcessor proc = new HashRecordProcessor(hcon);
 
    proc.process(new String[] { 
      Hex.encodeHexString(sha1), Hex.encodeHexString(md5),
      Hex.encodeHexString(crc32), name, String.valueOf(size),
      String.valueOf(prod_code), os_code, special_code
    });
  }
}
