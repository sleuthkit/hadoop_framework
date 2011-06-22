package org.sleuthkit.hadoop.scoring;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;

public class BytesArrayWritable extends ArrayWritable {
    public BytesArrayWritable() {
        super(BytesWritable.class);
    }
}
