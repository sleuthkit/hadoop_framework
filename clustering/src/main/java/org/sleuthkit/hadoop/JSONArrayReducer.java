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
                outArray.put((Integer)obj.get("a"), obj);
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (ClassCastException e) {
                // we should never get here, but there's no reason to bail if we do.
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
