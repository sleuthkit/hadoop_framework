/*
   Copyright 2011 Basis Technology Corp.

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

package org.sleuthkit.hadoop;

/** Contains constants which are used 
 *  as keys in FsEntry. These static strings provide a way to get
 *  at the associated data in FsEntry. For example, to get text
 *  extracted by tika, one would call FsEntry.get("sleuthkit.text).
 */
public class HBaseConstants {
    /** The text extracted through tika. This is a string. */
    public static final String EXTRACTED_TEXT = "sleuthkit.text";
    /** The text that has been matched by a regex. This is a JSONArray.
     * (ArrayList, via Jackson) */
    public static final String GREP_RESULTS = "sleuthkit.grep.results";
    /** The actual regex that was matched to a corresponding grep result.
     * This is a JSONArray (Arraylist, via Jackson). */
    public static final String GREP_SEARCHES = "sleuthkit.grep.regexes";
}
