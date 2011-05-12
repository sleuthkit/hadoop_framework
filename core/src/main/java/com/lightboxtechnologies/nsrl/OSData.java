package com.lightboxtechnologies.nsrl;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * An NSRL operating system record.
 *
 * @author Joel Uckelman
 */
public class OSData {
  public final String code;
  public final String name;
  public final String version;
  public final String mfg_code;

  public OSData(String code, String name, String version, String mfg_code) {
    if (code == null) throw new IllegalArgumentException();
    if (name == null) throw new IllegalArgumentException();
    if (version == null) throw new IllegalArgumentException();
    if (mfg_code == null) throw new IllegalArgumentException();

    this.code = code;
    this.name = name;
    this.version = version;
    this.mfg_code = mfg_code;
  }

  @Override
  public String toString() {
    return String.format(
      "%s[code=\"%s\",name=\"%s\",version=\"%s\",mfg_code=\"%s\"]",
      getClass().getName(), code, name, version, mfg_code
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OSData)) return false;
    final OSData d = (OSData) o;
    return code.equals(d.code) && name.equals(d.name) &&
           version.equals(d.version) && mfg_code.equals(d.mfg_code);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(code)
                                .append(name)
                                .append(version)
                                .append(mfg_code)
                                .toHashCode();
  }
}
