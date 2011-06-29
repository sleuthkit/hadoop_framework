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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Aggregates vector names and their associated clusters, and outputs JSON. */
public class JSONClusterNameMapper
extends Mapper<IntWritable, WeightedVectorWritable, NullWritable, Text> {
    public static final Logger LOG = LoggerFactory.getLogger(JSONClusterNameMapper.class);

    @Override
    public void map(IntWritable key, WeightedVectorWritable value, Context context)
    throws IOException, InterruptedException {
        String name = "";
        Vector v = value.getVector();
        if (v instanceof NamedVector) {
            name = ((NamedVector)v).getName();
        }

        JSONObject object = new JSONObject();
        try {
            object.put("a", key.get());
            object.put("fP", name);
            context.write(NullWritable.get(), new Text(object.toString()));
        } catch (JSONException e) {
            LOG.error("Error while creating JSON record.", e);
        }
    }
}