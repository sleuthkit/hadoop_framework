package com.lightboxtechnologies.test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.junit.Test;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

/**
 * A {@link Suite} which runs all tests in a directory tree.
 *
 * @author Joel Uckelman
 */
public abstract class DirectorySuite extends Suite {
  public DirectorySuite(Class<?> setupClass, String root)
                                                   throws InitializationError {
    super(setupClass, DirectorySuite.findTests(root));
  }

  protected static Class<?> loadClassByPath(String cpath) {
    // turn directory slashes into class hierarchy dots
    String cname = cpath.replace('/', '.');

    Class<?> c = null;
    try {
      return Class.forName(cname);
    }
    catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  protected static boolean hasTests(Class<?> c) {
    for (Method m : c.getMethods()) {
      if (m.getAnnotation(Test.class) != null) {
        return true;
      }
    }
    return false;
  }

  public static Class<?>[] findTests(String path) {
    final File root = new File(path);
    if (!root.isDirectory()) {
      throw new RuntimeException(root.getPath() + " is not a directory!");
    }

    final List<Class<?>> tests = new ArrayList<Class<?>>();

    final Deque<File> stack = new ArrayDeque<File>(); 
    stack.push(root);

    File dir;
    String cpath;

    while (!stack.isEmpty()) {
      dir = stack.pop();

      for (File f : dir.listFiles()) {
        if (f.isDirectory()) {
          stack.push(f); 
        }
        else {
          cpath = f.getPath();
          if (cpath.endsWith("Test.java")) {
            // strip off test root and ".java"
            cpath = cpath.substring(path.length()+1, cpath.length()-5);

            // take only classes which have @Test-annotated methods
            final Class<?> c = loadClassByPath(cpath);
            if (hasTests(c)) {
              tests.add(c);
            }
          }
        }
      }
    }

    return tests.toArray(new Class<?>[tests.size()]);
  }
}
