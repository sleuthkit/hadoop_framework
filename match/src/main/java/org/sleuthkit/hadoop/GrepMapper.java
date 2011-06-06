package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import org.codehaus.jackson.map.ObjectMapper;

import com.lightboxtechnologies.spectrum.FsEntry;

// Maps regex matches to an output file.
public class GrepMapper
extends SKMapper<Text, FsEntry, Text, FsEntry> {

    private List<Pattern> patterns = new ArrayList<Pattern>();
   
    private static final ObjectMapper mapper = new ObjectMapper();
 
    @Override
    public void setup(Context ctx) {
        String[] regexlist = ctx.getConfiguration().get("mapred.mapper.regex").split("\n");
        System.out.print("Found Regexes: " + regexlist.length);

        for (String item : regexlist) {
            if ("".equals(item)) continue; // don't add empty regexes
            try {
                patterns.add(Pattern.compile(item));
            } catch (Exception ex) {
                // not much to do...
            }
        }
        super.setup(ctx);
    }

    @Override
    public void map(Text key, FsEntry value, Context context)
    throws IOException {
        String text;
        try {
             text = (String)value.get(HBaseConstants.EXTRACTED_TEXT);
             if (text == null || text.equals("")) { return; }
        } catch (Exception ex) {
            System.err.println("No FsEntry for File: " + key.toString());
            return;
        }
        Set<String> s = new HashSet<String>();
        for (Pattern item : patterns) {
            Matcher matcher = item.matcher(text);
            
            while (matcher.find()) {
                s.add(matcher.group());
            }
        }
        
        try {
            final List<String> jl = new ArrayList(s);
            final String json = mapper.writeValueAsString(jl);
            value.put(HBaseConstants.GREP_RESULTS, json);
            
            context.write(key, value);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
