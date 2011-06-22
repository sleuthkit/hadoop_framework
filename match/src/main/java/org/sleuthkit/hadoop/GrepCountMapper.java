package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.ImmutableHexWritable;

public class GrepCountMapper 
extends SKMapper<ImmutableHexWritable, FsEntry, LongWritable, LongWritable>{

    final Logger LOG = LoggerFactory.getLogger(GrepCountMapper.class);

    @Override
    public void setup(Context ctx) {
        super.setup(ctx);
    }

    protected final LongWritable okey = new LongWritable();
    protected final LongWritable oval = new LongWritable(1);

    @Override
    public void map(ImmutableHexWritable key, FsEntry value, Context context)
    throws InterruptedException, IOException {

        @SuppressWarnings("unchecked")
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
