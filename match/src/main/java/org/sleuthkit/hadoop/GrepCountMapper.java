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

package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.ImmutableHexWritable;

/** Counts the number of grep matches we have for a drive, per keyword.
 *  Used for reporting. The key is the index in the grep search file.
 *  (The index is the line number the regex occurs on.)
 */
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
