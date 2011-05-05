package com.lightboxtechnologies.nsrl;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * An NSRL manufacturer record.
 * 
 * @author Joel Uckelman
 */
public class MfgData {
  public final String code;
  public final String name;

  public MfgData(String code, String name) {
    if (code == null) throw new IllegalArgumentException();
    if (name == null) throw new IllegalArgumentException();

    this.code = code;
    this.name = name;
  }

  @Override
  public String toString() {
    return String.format(
      "%s[code=\"%s\",name=\"%s\"]",
      getClass().getName(), code, name
    ); 
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MfgData)) return false;
    final MfgData d = (MfgData) o;
    return code.equals(d.code) && name.equals(d.name);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(code).append(name).toHashCode();
  }
}
