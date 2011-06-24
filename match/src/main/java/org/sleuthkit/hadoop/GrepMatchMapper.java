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
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.ImmutableHexWritable;

public class GrepMatchMapper 
extends SKMapper<ImmutableHexWritable, FsEntry, NullWritable, Text>{
    final Logger LOG = LoggerFactory.getLogger(GrepMatchMapper.class);
    
    final Text output = new Text();
    
    @Override
    public void setup(Context ctx) {
        super.setup(ctx);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void map(ImmutableHexWritable key, FsEntry value, Context context)
    throws InterruptedException, IOException {
        
        List<Object> grepKeywordList;
        List<String> grepSearchResults;

        grepKeywordList = (List<Object>)value.get(HBaseConstants.GREP_SEARCHES);
        grepSearchResults = (List<String>)value.get(HBaseConstants.GREP_RESULTS);
        
        if (grepKeywordList == null || grepSearchResults == null) { return; }
        // No grep results for this keyword...
        
        // There are grep results - generate json for them.
        for (int i = 0; i < grepKeywordList.size(); i++) {
            try {
                int a = (Integer)grepKeywordList.get(i);
                String ctx = grepSearchResults.get(i);
                String fP = value.fullPath();
                String fId = value.toString();

                JSONObject obj = new JSONObject();
                obj.put("a", a);
                obj.put("p", ctx);
                obj.put("fP", fP);
                obj.put("fId", fId);
                
                output.set(obj.toString());

                context.write(NullWritable.get(), output);
            } catch (JSONException ex) {
                LOG.error("Could not convert JSON to string.", ex);
            }
        }
        
    }
}
