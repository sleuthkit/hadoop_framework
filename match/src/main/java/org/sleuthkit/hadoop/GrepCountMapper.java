package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.ImmutableHexWritable;

public class GrepCountMapper 
extends SKMapper<ImmutableHexWritable, FsEntry, LongWritable, LongWritable>{

	@Override
	public void setup(Context ctx) {
	    super.setup(ctx);
	}

	@Override
	public void map(ImmutableHexWritable key, FsEntry value, Context context)
	throws InterruptedException, IOException {
	    try {
	        ArrayList<Object> grepKeywordList = (ArrayList)value.get(HBaseConstants.GREP_SEARCHES);
	        for (int i = 0; i < grepKeywordList.size(); i++) {
	            context.write(new LongWritable((int)(Integer)grepKeywordList.get(i)), new LongWritable(1));
	        }
	    } catch (Exception ex) {
	        // This is normal. If there are no grep matches, the array will be null.
	    }
	}
}
