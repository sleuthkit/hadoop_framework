package org.sleuthkit.hadoop;

public class HBaseConstants {
    public static final String EXTRACTED_TEXT = "sleuthkit.text";
    /** The text that has been matched by a regex. This is a JSONArray. */
    public static final String GREP_RESULTS = "sleuthkit.grep.results";
    /** The actual regex that was matched to a corresponding grep result.
     * This is a JSONArray. */
    public static final String GREP_SEARCHES = "sleuthkit.grep.regexes";
}
