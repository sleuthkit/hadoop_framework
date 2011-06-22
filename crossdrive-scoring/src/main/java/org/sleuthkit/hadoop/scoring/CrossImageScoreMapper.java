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
