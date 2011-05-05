
package com.lightboxtechnologies.spectrum;

import org.json.simple.*;
import org.json.simple.parser.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.io.*;

import org.apache.hadoop.fs.FileSystem;

import org.python.core.PyFile;

public class FsEntry extends HashMap<String,Object> {
  private static final long serialVersionUID = 1L;

  private boolean hasMRec;
  private String Path;
  private String Name;
  private Date   Created;
  private Date   Written;
  private Date   Accessed;
  private Date   Updated;
  private long   Size;

  private final Map<String,StreamProxy> Streams = new HashMap<String,StreamProxy>();

  private FileSystem FS;

  JSONParser Parser;

  public void clear() {
    Path = null;
    Name = null;
    super.clear();
  }

  public void setFileSystem(FileSystem fs) {
    FS = fs;
  }

  public Map<String, StreamProxy> getStreams() {
    return Streams;
  }

  public Object getStream() {
    return getStream("Content");
  }

  public Object getStream(String key) {
    StreamProxy val = Streams.get(key);
    if (val != null) {
      try {
        return new PyFile(val.open(FS));
      }
      catch (IOException ex) {}
    }
    return null;
  }

  public boolean hasMetadata() {
    return hasMRec;
  }

  public String getPath() {
    if (Path == null) {
      Path = (String)get("path");
    }
    return Path;
  }

  public String getName() {
    if (Name == null) {
      Name = (String)get("name");
    }
    return Name;
  }

  public String getID() {
    final StringBuilder buf = new StringBuilder(Path);
    buf.append(Name);
    buf.append("-");
    buf.append(get("dirIndex"));
    return buf.toString();
  }

  // always returns lowercase, which is The Right Thing.
  // this comment, (C) 2010, Geoff Black's Fear and Loathing
  public String extension() {
    final String s = getName();
    final int dot = s.lastIndexOf('.');
    if (-1 < dot && dot < s.length() - 1) {
      return s.substring(dot + 1).toLowerCase();
    }
    return "";
  }

  public String fullPath() {
    if (Path == null) {
      Path = (String)get("path");
    }

    if (Name == null) {
      Name = (String)get("name");
    }

    final StringBuilder buf = new StringBuilder(Path);
    if (buf.length() > 0 && '/' != buf.charAt(buf.length() - 1)) {
      buf.append('/');
    }
    buf.append(Name);
    return buf.toString();
  }

  public Date getCreated() {
    return Created;
  }

  public Date getWritten() {
    return Written;
  }

  public Date getAccessed() {
    return Accessed;
  }

  public Date getUpdated() {
    return Updated;
  }

  public long getSize() {
    return Size;
  }

  private static Date addDate(Map<String,Object> map, String name, JSONObject rec, String recName) {
    final long ts = ((Number)rec.get(recName)).longValue();
    if (ts > 0) {
      Date date = new Date(ts * 1000);
      map.put(name, date);
      return date;
    }
    return null;
  }

  private static void addOptLong(Map<String,Object> map, String name, JSONObject rec, String recName) {
    if (rec.containsKey(recName)) {
      map.put(name, rec.get(recName));
    }
  }

  public boolean parseJson(String jsonstr) {
    if (null == Parser) {
      Parser = new JSONParser();
    }

    JSONObject json = null;
    try {
      json = (JSONObject)Parser.parse(jsonstr);
    }
    catch (ParseException e) {
      return false;
    }

    final Map<String,Object> map = new HashMap<String,Object>();

    try {
      final JSONObject nRec = JSON.getAs(json, "name", JSONObject.class);
      final String p = JSON.getAs(json, "path", String.class);
      final String n = JSON.getAs(nRec, "name", String.class);

      if (json.containsKey("fs")) {
        final JSONObject fsRec = JSON.getAs(json, "fs", JSONObject.class);
        map.put("fs_byte_offset", fsRec.get("byteOffset"));
        map.put("fs_id", fsRec.get("fsID"));
        map.put("fs_block_size", fsRec.get("blockSize"));
      }

      long sz = 0;
      if (json.containsKey("meta")) {
        final JSONObject mRec = JSON.getAs(json, "meta", JSONObject.class);
        Created = addDate(map, "created", mRec, "crtime");
        Written = addDate(map, "written", mRec, "mtime");
        Accessed = addDate(map, "accessed", mRec, "atime");
        Updated = addDate(map, "updated", mRec, "ctime");
        sz = (JSON.getAs(mRec, "size", Number.class)).longValue();
        addOptLong(map, "meta_flags", mRec, "flags");
        addOptLong(map, "uid", mRec, "uid");
        addOptLong(map, "gid", mRec, "gid");
        addOptLong(map, "meta_type", mRec, "type");
        addOptLong(map, "seq", mRec, "seq");
        addOptLong(map, "mode", mRec, "mode");
        addOptLong(map, "content_len", mRec, "content_len");
        addOptLong(map, "meta_addr", mRec, "addr");
        addOptLong(map, "nlink", mRec, "nlink");
        hasMRec = true;
      }
      else {
        hasMRec = false;
      }

      if (json.containsKey("attrs")) {
        map.put("attrs", json.get("attrs"));
      }

      Path = p;
      Name = n;
      map.put("path", Path);
      map.put("name", Name);
      Size = sz;
      map.put("size", Size);
      map.put("dirIndex", nRec.get("dirIndex"));
      addOptLong(map, "name_flags", nRec, "flags");
      addOptLong(map, "meta_seq", nRec, "meta_seq");
      addOptLong(map, "name_type", nRec, "type");
      putAll(map);
      return true;
    }
    catch (JSON.DataException e) {
      return false;
    }
  }
}
