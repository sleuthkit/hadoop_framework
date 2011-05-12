/*
   Copyright 2011, Lightbox Technologies, Inc

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.lightboxtechnologies.nsrl;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * An NSRL product record.
 *
 * @author Joel Uckelman
 */
public class ProdData {
  public final int code;
  public final String name;
  public final String version;
  public final String os_code;
  public final String mfg_code;
  public final String language;
  public final String app_type;

  public ProdData(int code, String name, String version, String os_code,
                  String mfg_code, String language, String app_type) {
    if (code < 0) throw new IllegalArgumentException();
    if (name == null) throw new IllegalArgumentException();
    if (version == null) throw new IllegalArgumentException();
    if (os_code == null) throw new IllegalArgumentException();
    if (mfg_code == null) throw new IllegalArgumentException();
    if (language == null) throw new IllegalArgumentException();
    if (app_type == null) throw new IllegalArgumentException();

    this.code = code;
    this.name = name;
    this.version = version;
    this.os_code = os_code;
    this.mfg_code = mfg_code;
    this.language = language;
    this.app_type = app_type;
  }

  @Override
  public String toString() {
    return String.format(
      "%s[code=\"%d\",name=\"%s\",version=\"%s\",os_code=\"%s\",mfg_code=\"%s\",language=\"%s\",app_type=\"%s\"]",
      getClass().getName(),
      code, name, version, os_code, mfg_code, language, app_type
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ProdData)) return false;
    final ProdData d = (ProdData) o;
    return code == d.code && name.equals(d.name) &&
           version.equals(d.version) && os_code.equals(d.os_code) &&
           mfg_code.equals(d.mfg_code) && language.equals(d.language) &&
           app_type.equals(d.app_type);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(code)
                                .append(name)
                                .append(version)
                                .append(os_code)
                                .append(mfg_code)
                                .append(language)
                                .append(app_type)
                                .toHashCode();
  }
}
