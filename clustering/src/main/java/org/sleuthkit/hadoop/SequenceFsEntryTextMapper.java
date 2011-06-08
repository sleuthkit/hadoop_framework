package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.ImmutableHexWritable;

// Write out files from the HBase table to a sequence file IFF
// they have a regex match from the previous step.
public class SequenceFsEntryTextMapper
extends SKMapper<ImmutableHexWritable, FsEntry, ImmutableHexWritable, Text> {

    @Override
    public void map(ImmutableHexWritable key, FsEntry value, Context context)
    throws IOException {
        try {
            String output = (String)value.get(HBaseConstants.EXTRACTED_TEXT);
            if (output != null) {
                @SuppressWarnings("unchecked")
                List<Object> grepResults = (List<Object>)value.get(HBaseConstants.GREP_RESULTS);
                if ((grepResults != null) && (grepResults.size() > 0)) {
                    // There were grep results, so we are interested in this file.
                    context.write(key, new Text(output));
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
