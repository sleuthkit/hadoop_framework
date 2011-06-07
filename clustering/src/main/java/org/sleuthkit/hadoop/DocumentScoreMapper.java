package org.sleuthkit.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DocumentScoreMapper extends SKMapper<Text, ArrayWritable, Writable, DoubleWritable> {
    
    public static final String TOTAL_IMAGES = "org.sleuthkit.imagecount";
    public static final String FILES_IN_IMAGE = "org.sleuthkit.filecount";
    
    private int totalImages;
    private int filesInImage;
    
    @Override
    public void setup(Context context) {
        totalImages = context.getConfiguration().getInt(TOTAL_IMAGES, -1);
        filesInImage = context.getConfiguration().getInt(FILES_IN_IMAGE, -1);
    }
    
    @Override
    public void map(Text key, ArrayWritable value, Context context) throws IOException {
        
        Writable[] imagelist = value.get();
        int imageCount = imagelist.length;
        
        double iif = Math.log((double)totalImages / (double)imageCount);
        double iifNormalized = iif/(Math.log((double)totalImages) * filesInImage);
        DoubleWritable out = new DoubleWritable(iifNormalized);
        
        for (Writable imageID : imagelist) {
            try {
                context.write(imageID, out);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
