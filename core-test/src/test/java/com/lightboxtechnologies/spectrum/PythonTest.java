package com.lightboxtechnologies.spectrum;

import javax.script.*;
import java.io.*;

public class PythonTest {

  public void emit(Object key, Object value) {
    System.out.println("out key type = " + key.getClass().getName());
    System.out.println("out value type = " + value.getClass().getName());
  }

  public static void main(String[] args) throws ScriptException {
    ScriptEngine engine = new ScriptEngineManager().getEngineByName("python");
    FsEntry e = new FsEntry();
    e.parseJson("{\"path\":\"a/path/\", \"name\":{\"name\":\"aname.txt\",\"dirIndex\":2}, \"meta\":{\"size\":1, \"crtime\":258970620, \"mtime\":285249600, \"atime\":1123948800, \"ctime\":1261555200"
      + ", \"flags\":5,\"uid\":0,\"gid\":519,\"type\":2,\"seq\":3,\"mode\":511,\"content_len\":0,\"addr\":12345,\"nlink\":2}}");

    // e.setFile(args[1]);

    try {
      FileReader script = new FileReader(args[0]);
      engine.eval(script);
      String keyType = (String)engine.get("keyType"),
             valueType = (String)engine.get("valueType");
      System.out.println("keyType = " + keyType);
      System.out.println("valueType = " + valueType);
      Invocable py = (Invocable)engine;
      PythonTest p = new PythonTest();
      py.invokeFunction("mapper", "whatever", e, p);
    }
    catch (Exception ex) {
      System.err.println(ex.toString());
    }
  }
}
