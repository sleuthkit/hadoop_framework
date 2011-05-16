/*
src/com/lightboxtechnologies/spectrum/FsEntryHBaseInputFormat.java

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

package com.lightboxtechnologies.spectrum;

import org.apache.hadoop.hbase.util.*;

public class HBaseTables {

  static public final String ENTRIES_TBL = "entries";
  static public final byte[] ENTRIES_TBL_B = Bytes.toBytes(ENTRIES_TBL);

  static public final String ENTRIES_COLFAM = "core";
  static public final byte[] ENTRIES_COLFAM_B = Bytes.toBytes(ENTRIES_COLFAM);

  static public final String HASH_TBL = "hash";
  static public final byte[] HASH_TBL_B = Bytes.toBytes(HASH_TBL);
}
