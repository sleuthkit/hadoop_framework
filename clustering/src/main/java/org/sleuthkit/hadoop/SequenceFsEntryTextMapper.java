package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;

import com.lightboxtechnologies.spectrum.FsEntry;

// Write out files from the HBase table to a sequence file IFF
// they have a regex match from the previous step.
public class SequenceFsEntryTextMapper
extends SKMapper<Text, FsEntry, Text, Text> {

    @Override
    public void map(Text key, FsEntry value, Context context)
    throws IOException {
        try {
            String output = (String)value.get(HBaseConstants.EXTRACTED_TEXT);
            if (output != null) {
                
                ArrayList<Object> grepResults;
                try {
                    grepResults = (ArrayList)value.get(HBaseConstants.GREP_RESULTS);
                } catch (NullPointerException ex) {
                    // This is common. There were no grep results. Just bail.
                    return;
                }
                if ((grepResults != null) && (grepResults.size() > 0)) {
                    context.write(key, new Text(output));
                }
                
            }
            else {
                //System.out.println("Warning: No text for key: " + key.toString());
            }

            
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
