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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.math.Vector;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Generates text listing the top 10 features of the vectors we are
// using as centroids for our clusters.
public class TopFeatureMapper
extends Mapper<Text, Cluster, NullWritable, Text> {
    final Logger LOG = LoggerFactory.getLogger(TopFeatureMapper.class);

    String[] dictionaryVector;

    @Override
    public void setup(Context context) {
        String path = context.getConfiguration().get("org.sleuthkit.hadoop.dictionary");

        try {
            dictionaryVector = ClusterUtil.loadTermDictionary(context.getConfiguration(), FileSystem.get(context.getConfiguration()), path);
        } catch (IOException e) {
            LOG.error("IOException while attempting to get the dictionary vector.", e);
        }
    }

    @Override
    public void map(Text key, Cluster value, Context context)
    throws IOException, InterruptedException {
        Vector v = value.getCenter();
        value.getNumPoints();
        
        JSONObject obj = new JSONObject();
        try {
            int i = Integer.parseInt(key.toString().substring(3));
            obj.put("a", i);
            obj.put("n", value.getNumPoints());
            obj.put("kw", ClusterUtil.getTopFeatures(v, dictionaryVector, 10));
        } catch (JSONException ex) {
            LOG.error("Exception while attempting to create JSON object.", ex);
        } catch (NumberFormatException ex) {
            LOG.error("Could not Parse Cluster name to number.", ex);
        }

        context.write(NullWritable.get(), new Text(obj.toString()));

    }

}
