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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.ImmutableHexWritable;

// Maps regex matches to an output file.
public class GrepMapper
extends SKMapper<ImmutableHexWritable, FsEntry, ImmutableHexWritable, FsEntry> {
    Logger LOG = LoggerFactory.getLogger(GrepMapper.class);
    private List<Pattern> patterns = new ArrayList<Pattern>();
    
    @Override
    public void setup(Context ctx) {
        String[] regexlist = ctx.getConfiguration().get("mapred.mapper.regex").split("\n");
        System.out.print("Found Regexes: " + regexlist.length);

        for (String item : regexlist) {
            if ("".equals(item)) continue; // don't add empty regexes
            try {
                patterns.add(Pattern.compile(item));
            } catch (Exception ex) {
                // TODO: Replace this with something better.
                // Why do this? Pattern indices matter this this implementation.
                // If a pattern is not added properly, that will throw off our
                // indexing later on in the map step. These will also be logged.
                patterns.add(Pattern.compile("xxxINVALID_PATTERNxxx"));
                LOG.error("Bad regular expression: " + item, ex);   
                
            }
        }
        super.setup(ctx);
    }

    @Override
    public void map(ImmutableHexWritable key, FsEntry value, Context context)
    throws IOException {
        String text;
        try {
             text = (String)value.get(HBaseConstants.EXTRACTED_TEXT);
             if (text == null || text.equals("")) { return; }
        } catch (Exception ex) {
            System.err.println("No FsEntry for File: " + key.toString());
            return;
        }
        // The list of actual matches.
        List<String> s = new ArrayList<String>();
        // The list of regexes matched.
        List<Integer> g = new ArrayList<Integer>();
        // TODO: We could have a context variable here as well, storing info
        // about the text surrounding the grep match.
        int i = 0;
        for (i = 0; i < patterns.size(); i++) {
            Pattern item = patterns.get(i);
            Matcher matcher = item.matcher(text);
            
            while (matcher.find()) {
                s.add(matcher.group());
                g.add(i);
            }
        }
        
        try {
            JSONArray ar = new JSONArray(s);
            JSONArray grepMatches = new JSONArray(g);
            
            value.put(HBaseConstants.GREP_RESULTS, ar.toString());
            value.put(HBaseConstants.GREP_SEARCHES, grepMatches.toString());
            context.write(key, value);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}