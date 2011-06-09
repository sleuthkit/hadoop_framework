package org.sleuthkit.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

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
