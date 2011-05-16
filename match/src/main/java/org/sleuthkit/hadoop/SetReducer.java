package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Reduces a class using a java set to remove duplicates.
public  class SetReducer extends Reducer<Text, Text, Text, ArrayWritable>{
    @Override
    public void reduce(Text key, Iterable<Text> values,
            Context ctx)
    throws IOException {

        Set<String> s = new HashSet<String>();

        for (Text t : values) {
            s.add(t.toString());
        }
        String[] str = new String[1];
        try {
            ctx.write(key, new ArrayWritable(s.toArray(str)));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}