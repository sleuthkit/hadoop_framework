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
import java.util.HashSet;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.sleuthkit.hadoop.SKMapper;

public class CrossImageScoreReducer extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesArrayWritable> {
    byte[] ourImageID;
    @Override
    public void setup(Context context) {
        try {
            ourImageID = Hex.decodeHex(context.getConfiguration().get(SKMapper.ID_KEY).toCharArray());
        } catch (DecoderException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void reduce(BytesWritable fileHash, Iterable<BytesWritable> imgIDs, Context context) throws IOException, InterruptedException {
        boolean inThisImage = false;
        BytesArrayWritable aw = new BytesArrayWritable();
        HashSet<Writable> valueList = new HashSet<Writable>();
        for (BytesWritable curImgID : imgIDs) {
            if (belongsToImage(curImgID.getBytes())) {
                System.out.println("Hashes equal: " + new String(Hex.encodeHex(curImgID.getBytes())) + " to " + Hex.encodeHexString(ourImageID));
                inThisImage = true;
            }
            valueList.add(new BytesWritable(curImgID.getBytes().clone()));
        }
        
        if (inThisImage) {
            aw.set(valueList.toArray(new BytesWritable[0]));
            context.write(fileHash, aw);
            System.out.println("Done Writing Context.");
        }
    }
    

    public boolean belongsToImage(byte[] imgID) {
        // there are often trailing zeroes on the byte array returned by
        // hadoop. This code is intended to help account that problem by
        // ensuring that the relevant bytes match.
        if (imgID.length < ourImageID.length) {
            return false;
        }
        for (int i = 0; i < ourImageID.length; i++) {
            if (imgID[i] != ourImageID[i]) {
                return false;
            }
        }
        return true;
    }
    

}
