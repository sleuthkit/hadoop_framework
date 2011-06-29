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
import java.util.Arrays;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.BytesWritable;
import org.sleuthkit.hadoop.SKMapper;

import com.lightboxtechnologies.spectrum.KeyUtils;

/** Iterates over the hashes table, finds all file+imageID hashes, and emits a
 * hash to image ID mapping.
 */
public class CrossImageScoreMapper extends TableMapper<BytesWritable, BytesWritable>{
    
    byte[] imgID = new byte[16];
    byte[] hash = new byte[16];
    
    BytesWritable imgIDBytes = new BytesWritable();
    BytesWritable hashBytes = new BytesWritable();
    
    byte[] ourImgID;
        
    @Override
    public void setup(Context context) {
        try {
            ourImgID = Hex.decodeHex(context.getConfiguration().get(SKMapper.ID_KEY).toCharArray());
        } catch (DecoderException e) {
            throw new RuntimeException("Could not parse key to hex! ", e);
        }
    }
    
    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] keyArray = key.get();
        
        // We want type 2 MD5 keys. Ignore type 1 and SHA1 keys.
        if (KeyUtils.isType2(keyArray) && (KeyUtils.getHashLength(keyArray) == 16)) {
            // This key contains has information.
            KeyUtils.getImageID(imgID, keyArray);
            KeyUtils.getHash(hash, keyArray);
            imgIDBytes.set(imgID, 0, 16);
            hashBytes.set(hash, 0, 16);
            
            if (Arrays.equals(imgID, ourImgID)) {
                context.getCounter(CrossImageScorerJob.FileCount.FILES).increment(1);
            }

            context.write(hashBytes, imgIDBytes);
        }
    }
    
    
}
