package org.sleuthkit.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONArray;

import com.lightboxtechnologies.spectrum.FsEntry;

// Maps regex matches to an output file.
public class SequenceFsEntryTextMapper
extends SKMapper<Text, FsEntry, Text, Text> {

    @Override
    public void map(Text key, FsEntry value, Context context)
    throws IOException {
        try {
            String output = (String)value.get(HBaseConstants.EXTRACTED_TEXT);
            if (output != null) {
                String fsResults = context.getConfiguration().get(SequenceFsEntryText.GREP_MATCHES_TO_SEARCH);
                if (fsResults == null || fsResults.equals("")) { return; }
                
                JSONArray grepResults = (JSONArray)value.get(fsResults);
                if ((grepResults != null) && (grepResults.size() > 0)) {
                    context.write(key, new Text(output));
                }
                
            }
            else {
                System.out.println("Warning: No text for key: " + key.toString());
            }

            
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
