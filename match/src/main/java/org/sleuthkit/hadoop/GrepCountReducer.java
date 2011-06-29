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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/** Sums the numbmer of times a given item has been found through grep searching.
 * Outputs that data to a JSON array. This is then packaged into an output report.
 */
public class GrepCountReducer extends Reducer<LongWritable, LongWritable, NullWritable, Text> {

    JSONArray outArray = new JSONArray();
    String regexes[];

    @Override
    protected void setup(Context context) {
        // TODO: We could make sure that we only have one instance of this
        // here, since that's all I'm supporting at the moment.
        regexes = context.getConfiguration().get("mapred.mapper.regex").split("\n");
    }

    @SuppressWarnings("unchecked")
    @Override 
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) {
        JSONObject obj = new JSONObject();
        int sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        obj.put("a", key.get());
        obj.put("n", sum);
        obj.put("kw", regexes[(int) key.get()]);

        outArray.add(obj);
    }

    @Override
    protected void cleanup(Context context)
                                     throws IOException, InterruptedException {
        context.write(NullWritable.get(), new Text(outArray.toJSONString()));
    }
}
