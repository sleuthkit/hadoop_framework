package org.sleuthkit.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/** A lightweight reducer intended to make a json array with each element of
 * the array being represented by the text passed into it from the mappers.
 *
 */
public class JSONArrayReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    JSONArray outArray = new JSONArray();
    
    @Override
    protected void setup(Context context) {

    }
    @Override 
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) {
        for (Text value : values) {
            JSONObject obj;
            try {
                obj = new JSONObject(value.toString());
                outArray.put(obj);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void cleanup(Context context) {
        try {
            context.write(NullWritable.get(), new Text(outArray.toString()));
        } catch (Exception e) {
            
        }
    }

}
