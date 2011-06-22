package org.sleuthkit.hadoop.scoring;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.sleuthkit.hadoop.SKMapper;

public class DocumentScoreMapper extends SKMapper<BytesWritable, BytesArrayWritable, Writable, DoubleWritable> {
    
    public static final String TOTAL_IMAGES = "org.sleuthkit.imagecount";
    public static final String FILES_IN_IMAGE = "org.sleuthkit.filecount";

    private long totalImages;
    private long filesInImage;
    
    private DoubleWritable out = new DoubleWritable();
    
    @Override
    public void setup(Context context) {
        totalImages = context.getConfiguration().getLong(TOTAL_IMAGES, -1);
        filesInImage = context.getConfiguration().getLong(FILES_IN_IMAGE, -1);
    }
    
    @Override
    public void map(BytesWritable key, BytesArrayWritable value, Context context) throws IOException, InterruptedException {
        Writable[] imagelist = value.get();
        int imageCount = imagelist.length;
        double iif = Math.log((double)totalImages / (double)imageCount);
        double iifNormalized = iif/(Math.log((double)totalImages) * (double)filesInImage);
        out.set(iifNormalized);

        for (Writable imageID : imagelist) {
            context.write(imageID, out);
        }
    }
}
