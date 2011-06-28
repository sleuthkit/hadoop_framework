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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A lightweight reducer intended to make a json array with each element of
 * the array being represented by the text passed into it from the mappers.
 *
 */
public class JSONArrayReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    public static final Logger LOG = LoggerFactory.getLogger(JSONArrayReducer.class);
    JSONArray outArray = new JSONArray();

    @Override 
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) {
        for (Text value : values) {
            JSONObject obj;
            try {
                obj = new JSONObject(value.toString());
                outArray.put((Integer)obj.get("a"), obj);
            } catch (JSONException e) {
                LOG.error("Could not add JSON Data to object.", e);
            } catch (ClassCastException e) {
               LOG.error("Error while parsing data to JSON.", e);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws InterruptedException, IOException {
        context.write(NullWritable.get(), new Text(outArray.toString()));
    }

}
