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

public class GrepMatchReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    JSONArray outArray = new JSONArray();
    String regexes[];

    @Override
    protected void setup(Context context) {
        // TODO: We could make sure that we only have one instance of this
        // here, since that's all I'm supporting at the moment.\
        try {
            regexes = context.getConfiguration().get("mapred.mapper.regex").split("\n");
        } catch (Exception ex) {
            
        }
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
    protected void cleanup(Context context)
                                     throws IOException, InterruptedException {
        context.write(NullWritable.get(), new Text(outArray.toString()));
    }

}
