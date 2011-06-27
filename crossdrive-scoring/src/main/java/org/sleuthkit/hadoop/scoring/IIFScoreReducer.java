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

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class IIFScoreReducer extends Reducer<Writable, DoubleWritable, NullWritable, Text>{
    DoubleWritable scoreWritable = new DoubleWritable();
    
    public enum iifCounter {COUNT};
    
    
    public static final String TOTAL_IMAGES = "org.sleuthkit.imagecount";
    public static final String FILES_IN_IMAGE = "org.sleuthkit.filecount";

    private long totalImages;
    private long filesInImage;
    
    JSONArray output = new JSONArray();
    
    
    @Override
    public void setup(Context context) {
        totalImages = context.getConfiguration().getLong(TOTAL_IMAGES, -1);
        filesInImage = context.getConfiguration().getLong(FILES_IN_IMAGE, -1);
    }
    @Override
    public void reduce(Writable key, Iterable<DoubleWritable> values, Context context) {
        JSONObject outputRecord = new JSONObject();
        double iif = 0;
        for (DoubleWritable iifu : values) {
            context.getCounter(iifCounter.COUNT).increment(1);
            iif = iif + iifu.get();
        }
        
        double confidence = iif/(Math.log((double)totalImages) * (double)filesInImage);
        
        try {
            outputRecord.put("id", new String(Hex.encodeHex(((BytesWritable)key).getBytes())));
            outputRecord.put("c", confidence);
            outputRecord.put("iif", iif);
            output.put(outputRecord);
        } catch (JSONException ex) { 
            ex.printStackTrace();
        }
    }
    
    @Override
    protected void cleanup(Context context) {
        try {
            context.write(NullWritable.get(), new Text(output.toString()));
        } catch (Exception e) {
            
        }
    }


}
