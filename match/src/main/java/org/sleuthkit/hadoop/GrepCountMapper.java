package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.ImmutableHexWritable;

public class GrepCountMapper 
extends SKMapper<ImmutableHexWritable, FsEntry, LongWritable, LongWritable>{

	@Override
	public void setup(Context ctx) {
	  super.setup(ctx);
	}

  protected final LongWritable okey = new LongWritable();
  protected final LongWritable oval = new LongWritable(1);

	@Override
	public void map(ImmutableHexWritable key, FsEntry value, Context context)
	                                   throws InterruptedException, IOException {
	  final List<Integer> grepKeywordList =
        (List<Integer>) value.get(HBaseConstants.GREP_SEARCHES);

    if (grepKeywordList != null) {
      for (Integer i : grepKeywordList) {
        okey.set(i);
	      context.write(okey, oval);
	    }
	  }
	}
}
