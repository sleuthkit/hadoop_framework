package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import com.lightboxtechnologies.spectrum.FsEntry;

// Maps regex matches to an output file.
public class SequenceFsEntryTextMapper
extends SKMapper<Text, FsEntry, Text, Text> {

    @Override
    public void map(Text key, FsEntry value, Context context)
    throws IOException {
        try {
            String output = (String)value.get("sleuthkit.text");
            if (output != null) {
                context.write(key, new Text(output));
            }
            else {
                System.out.println("Warning: No text for key: " + key.toString());
            }

            
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
