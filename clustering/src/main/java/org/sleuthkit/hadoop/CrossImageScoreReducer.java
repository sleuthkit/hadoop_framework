package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class CrossImageScoreReducer extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesArrayWritable> {
    byte[] imageID;
    @Override
    public void setup(Context context) {
        try {
            imageID = Hex.decodeHex(context.getConfiguration().get(SKMapper.ID_KEY).toCharArray());
        } catch (DecoderException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        boolean inThisImage = false;
        BytesArrayWritable aw = new BytesArrayWritable();
        HashSet<Writable> valueList = new HashSet<Writable>();
        for (BytesWritable w : values) {
            if (belongsToImage(w.getBytes())) {
                inThisImage = true;
            }
            valueList.add(w);
        }
        
        if (inThisImage) {
            aw.set(valueList.toArray(new BytesWritable[0]));
            context.write(key, aw);
        }
    }
    

    public boolean belongsToImage(byte[] hash) {
        // there are often trailing zeroes on the byte array returned by
        // hadoop. This code is intended to help account that problem by
        // ensuring that the relevant bytes match.
        if (hash.length < imageID.length) {
            return false;
        }
        for (int i = 0; i < imageID.length; i++) {
            if (hash[i] != imageID[i]) {
                return false;
            }
        }
        return true;
    }
    

}
