package org.sleuthkit.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class DoubleSumReducer extends Reducer<Writable, DoubleWritable, Writable, DoubleWritable>{
    
    @Override
    public void reduce(Writable key, Iterable<DoubleWritable> values, Context context) {
        double score = 0;
        for (DoubleWritable iif : values) {
            score = score + iif.get();
        }
        
        try {
            context.write(key, new DoubleWritable(score));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
