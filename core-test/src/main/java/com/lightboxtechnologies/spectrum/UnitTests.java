package com.lightboxtechnologies.spectrum;

import org.junit.runner.RunWith;
import org.junit.runners.model.InitializationError;

import com.lightboxtechnologies.test.DirectorySuite;

@RunWith(UnitTests.AllSuite.class)
public class UnitTests {
  public static class AllSuite extends DirectorySuite {
    public AllSuite(Class<?> setupClass) throws InitializationError {
      super(setupClass, "test");
    }
  }
}
