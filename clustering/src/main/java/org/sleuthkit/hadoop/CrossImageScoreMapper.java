package org.sleuthkit.hadoop;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.mahout.math.Arrays;

public class CrossImageScoreMapper extends TableMapper<String, String>{

    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException {
        if (key.getLength() <= 128) {
            // This key contains no information about the md5 hash; skip it.
            return;
        }
        else {
            // This key contains has information.
            byte[] keyArray = key.get();
            byte[] hash = Arrays.copyOf(keyArray, 128);
            //byte[] fileName = Arrays
        }
    }
}
