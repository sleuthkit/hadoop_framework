package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.ImmutableHexWritable;

public class GrepMatchMapper 
extends SKMapper<ImmutableHexWritable, FsEntry, NullWritable, Text>{

    @Override
    public void setup(Context ctx) {
        super.setup(ctx);
    }

    @Override
    public void map(ImmutableHexWritable key, FsEntry value, Context context)
    throws InterruptedException, IOException {
        ArrayList<Object> grepKeywordList;
        ArrayList<String> grepSearchResults;

        grepKeywordList = (ArrayList)value.get(HBaseConstants.GREP_SEARCHES);
        grepSearchResults = (ArrayList)value.get(HBaseConstants.GREP_RESULTS);
        
        if (grepKeywordList == null || grepSearchResults == null) { return; }
        // No grep results for this keyword...
        
        for (int i = 0; i < grepKeywordList.size(); i++) {
            try {
               
                int a = (Integer)grepKeywordList.get(i);
                String ctx = (String)grepSearchResults.get(i);
                String fid = value.getPath();

                JSONObject obj = new JSONObject();
                obj.put("a", a);
                obj.put("p", ctx);
                obj.put("fid", fid);

                context.write(NullWritable.get(), new Text(obj.toString()));
            } catch (JSONException ex) {
                ex.printStackTrace();
            }
        }
        
    }
}
