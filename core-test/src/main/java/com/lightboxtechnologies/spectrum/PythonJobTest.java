package com.lightboxtechnologies.spectrum;

import org.junit.Test;
import static org.junit.Assert.*;

import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import com.lightboxtechnologies.spectrum.PythonJob.*;

public class PythonJobTest {
  @Test
  public void testTextBoxerUnboxer() {
    final TextBoxerUnboxer ts = new TextBoxerUnboxer();
    final String s = "whatever";
    assertEquals(s, ts.set(s).toString());
    assertEquals(Text.class, ts.getBoxClass());
  }

  @Test
  public void testLongBoxerUnboxer() {
    final LongBoxerUnboxer ls = new LongBoxerUnboxer();
    final long l = 54;
    assertEquals(l, ls.set(l).get());
    assertEquals(LongWritable.class, ls.getBoxClass());
  }

  @Test
  public void testDoubleBoxerUnboxer() {
    final DoubleBoxerUnboxer d = new DoubleBoxerUnboxer();
    assertEquals(Math.PI, d.set(Math.PI).get(), 0.000001);
    assertEquals(DoubleWritable.class, d.getBoxClass());
  }

  @Test
  public void testCreateText() {
    assertTrue(PythonJob.createOutputType("text") instanceof TextBoxerUnboxer);
  }

  @Test
  public void testCreateLong() {
    assertTrue(PythonJob.createOutputType("long") instanceof LongBoxerUnboxer);
  }

  @Test
  public void testCreateFailure() {
    assertEquals(null, PythonJob.createOutputType("string"));
  }

  @Test
  public void testCreateDouble() {
    assertTrue(PythonJob.createOutputType("double") instanceof DoubleBoxerUnboxer);
  }

  @Test
  public void testCreateJson() {
    assertTrue(PythonJob.createOutputType("json") instanceof JsonBoxerUnboxer);
  }

  @Test
  public void testUnboxDouble() {
    DoubleBoxerUnboxer d = new DoubleBoxerUnboxer();
    assertEquals(3.14159, d.unbox(new DoubleWritable(3.14159)), 0.000001);
  }
}
