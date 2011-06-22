package org.sleuthkit.hadoop.scoring;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class DoubleSumReducer extends Reducer<Writable, DoubleWritable, Writable, DoubleWritable>{
    DoubleWritable scoreWritable = new DoubleWritable();
    @Override
    public void reduce(Writable key, Iterable<DoubleWritable> values, Context context) {
        double score = 0;
        for (DoubleWritable iif : values) {
            score = score + iif.get();
        }
        
        try {
            scoreWritable.set(score);
            context.write(key, scoreWritable);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
