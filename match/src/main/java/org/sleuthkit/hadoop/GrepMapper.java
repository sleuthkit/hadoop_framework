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
import java.util.regex.PatternSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.ImmutableHexWritable;

/** Searches the file text in a given hard drive for the regexes
 * provided by mapred.mapper.regex. Outputs the data back into FsEntry,
 * so it is stored in the file table (assuming the output task is set properly).
 */
public class GrepMapper
extends SKMapper<ImmutableHexWritable, FsEntry, ImmutableHexWritable, FsEntry> {
    final Logger LOG = LoggerFactory.getLogger(GrepMapper.class);
    private List<Pattern> patterns = new ArrayList<Pattern>();
    
    @Override
    public void setup(Context ctx) {
        String[] regexlist = ctx.getConfiguration().get("mapred.mapper.regex").split("\n");
        for (String item : regexlist) {
            if ("".equals(item)) continue; // don't add empty regexes
            try {
                patterns.add(Pattern.compile(item));
            } catch (PatternSyntaxException ex) {
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
    throws IOException, InterruptedException {
        String text;
        try {
             text = (String)value.get(HBaseConstants.EXTRACTED_TEXT);
             if (text == null || text.equals("")) { return; }
        } catch (NullPointerException ex) {
            System.err.println("No FsEntry for File: " + key.toString());
            return;
        }
        // The list of actual matches.
        List<String> s = new ArrayList<String>();
        // The list of regexes matched.
        List<Integer> g = new ArrayList<Integer>();
        int i = 0;
        for (i = 0; i < patterns.size(); i++) {
            Pattern item = patterns.get(i);
            Matcher matcher = item.matcher(text);
            
            while (matcher.find()) {
                int start = matcher.start();
                int end = matcher.end();
                s.add(text.substring(start < 20 ? 0 : start - 20, end + 20 > text.length() ? text.length() : end + 20));
                g.add(i);
            }
        }
        value.put(HBaseConstants.GREP_RESULTS, s);
        value.put(HBaseConstants.GREP_SEARCHES, g);
        context.write(key, value);
    }
}
