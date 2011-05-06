package com.lightboxtechnologies.spectrum;

import org.junit.Test;
import static org.junit.Assert.*;

public class ExtractMapperTest {
  @Test
  public void testHashFolder() {
    assertEquals(
      "aa/bb", ExtractMapper.hashFolder("aabbccddeeff00112233445566778899")
    );
  }
}
