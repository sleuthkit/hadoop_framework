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

package org.sleuthkit.hadoop.scoring;

import java.io.IOException;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.sleuthkit.hadoop.SKMapper;

public class DocumentScoreMapper extends SKMapper<BytesWritable, BytesArrayWritable, Writable, DoubleWritable> {
    
    public enum TestWritableCount {WRITEOUTS};

    private DoubleWritable out = new DoubleWritable();
    
    public static final String TOTAL_IMAGES = "org.sleuthkit.imagecount";

    private long totalImages;
    
    
    @Override
    public void setup(Context context) {
        totalImages = context.getConfiguration().getLong(TOTAL_IMAGES, -1);
    }
    
    @Override
    public void map(BytesWritable key, BytesArrayWritable imgListWritable, Context context) throws IOException, InterruptedException {
        Writable[] imagelist = imgListWritable.get();
        int imageCount = imagelist.length;
        double iif = Math.log((double)totalImages / (double)imageCount);
        out.set(iif);

        for (Writable imageID : imagelist) {
            // TODO: Don't add ourself to the score for a given image
            System.out.println("Image:" + new String(Hex.encodeHex(((BytesWritable)imageID).getBytes())) + " SCORE: " + String.valueOf(iif));
            // count number of written-out values
            context.getCounter(TestWritableCount.WRITEOUTS).increment(1);
            ((BytesWritable)imageID).setSize(16);
            context.write(imageID, out);
        }
    }
}
