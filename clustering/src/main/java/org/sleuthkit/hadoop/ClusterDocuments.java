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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ClusterDocuments {


    public static int runPipeline(String input, String output, String dictionary, double t1, double t2, String imageID, String friendlyName) {
        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx4096m");
        Path canopyInputPath = new Path(input);
        Path canopyOutputPath = new Path(output + "/canopy");

        Path kmeansInputPath = new Path(input);
        Path kmeansOutputPath = new Path(output + "/kmeans");
        // Canopy (I'm quite certain) only does one pass, so the relevant
        // clusters should be found in this file. For KMeans, this may not
        // be the case. Note, though, that the final clusters with document
        // vectors will be in a different file.
        Path kmeansClusters = new Path(output + "/canopy/clusters-0");

        try {
            CanopyDriver.run(conf, canopyInputPath, canopyOutputPath, new CosineDistanceMeasure(), t1, t2, true, false);
        } catch (Exception e) {
            return 1;
        }

        try {
            KMeansDriver.run(conf, kmeansInputPath, kmeansClusters, kmeansOutputPath, new CosineDistanceMeasure(), .5, 20, true, false);
        } catch (Exception e) {
            return 2;
        }

        try {
            // Map vector names to clusters
            // This is currently unused. Commenting in case we find a use for it.
//            Job job = SKJobFactory.createJob(imageID, friendlyName, "VectorNamesToClusters");
//            job.setJarByClass(ClusterDocuments.class);
//
//            FileInputFormat.setInputPaths(job, new Path(output + "/kmeans/clusteredPoints/"));
//            FileOutputFormat.setOutputPath(job, new Path(output + "/kmeans/reducedClusters/"));
//
//            job.setMapperClass(ClusterDocuments.ClusterMapper.class);
//            job.setMapOutputKeyClass(IntWritable.class);
//            job.setMapOutputValueClass(Text.class);
//
//            job.setReducerClass(ClusterDocuments.SetReducer.class);
//            job.setOutputKeyClass(IntWritable.class);
//            job.setOutputValueClass(ArrayWritable.class);
//
//            job.setInputFormatClass(SequenceFileInputFormat.class);
//            job.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//            job.waitForCompletion(true);

            ////////////////////////////////
            // Output top cluster matches //
            ////////////////////////////////
            Job job = SKJobFactory.createJob(imageID, friendlyName, "TopClusterMatchPrinting");
            job.setJarByClass(ClusterDocuments.class);


            // Get the final kmeans iteration. This is sort of a pain but for
            // whatever reason hadoop has no mechanism to do this for us.
            FileSystem fs = FileSystem.get(job.getConfiguration());
            int i = 2;
            Path goodPath = new Path(output + "/kmeans/clusters-1");

            while (true) {
                Path testPath = new Path(output + "/kmeans/clusters-" + i);
                if (!fs.exists(testPath)) {
                    break;
                }
                i++;
                goodPath = testPath;
            }
            
            FileInputFormat.setInputPaths(job, goodPath);
            FileOutputFormat.setOutputPath(job, new Path(output + "/kmeans/topClusters/"));

            job.setMapperClass(ClusterDocuments.TopPointsMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            // We need to reduce serially.
            job.setNumReduceTasks(1);

            job.setReducerClass(JSONArrayReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.getConfiguration().set("org.sleuthkit.hadoop.dictionary", dictionary);
            
            job.waitForCompletion(true);
            
            ////////////////////////////////
            // Output Cluster->DocID JSON //
            ////////////////////////////////
            
            job = SKJobFactory.createJob(imageID, friendlyName, "ClusteredVectorsToJson");
            job.setJarByClass(ClusterDocuments.class);

            FileInputFormat.setInputPaths(job, new Path(output + "/kmeans/clusteredPoints/"));
            FileOutputFormat.setOutputPath(job, new Path(output + "/kmeans/jsonClusteredPoints/"));

            job.setMapperClass(ClusterDocuments.JSONClusterMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setNumReduceTasks(1);
            job.setReducerClass(JSONArrayReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);
            
            
            ClusterJSONBuilder.buildReport(
                    new Path(output + "/kmeans/topClusters/part-r-00000"), 
                    new Path(output + "/kmeans/jsonClusteredPoints/part-r-00000"),
                    new Path(output + "/kmeans/json"));
            return 0;
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }

        return 0;
    }
    

    // Aggregates vector names and their associated clusters
    public static class ClusterMapper
    extends Mapper<IntWritable, WeightedVectorWritable, IntWritable, Text> {

        @Override
        public void map(IntWritable key, WeightedVectorWritable value, Context context)
        throws IOException {
            String name = "";
            Vector v = value.getVector();
            if (v instanceof NamedVector) {
                name =((NamedVector)v).getName();
            }

            try {
                context.write(key, new Text(name));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    
    // Aggregates vector names and their associated clusters, and outputs JSON.
    public static class JSONClusterMapper
    extends Mapper<IntWritable, WeightedVectorWritable, NullWritable, Text> {

        @Override
        public void map(IntWritable key, WeightedVectorWritable value, Context context)
        throws IOException {
            String name = "";
            Vector v = value.getVector();
            if (v instanceof NamedVector) {
                name =((NamedVector)v).getName();
            }

            try {
                JSONObject object = new JSONObject();
                object.put("a", key.get());
                object.put("fP", name);
                context.write(NullWritable.get(), new Text(object.toString()));
                //context.write(key, new Text(name));
                //context.write(new Text(), new Text("{a:" + key + ", fP:\"" + name + "\"},"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Generates text listing the top 10 features of the vectors we are
    // using as centroids for our clusters.
    public static class TopPointsMapper
    extends Mapper<Text, Cluster, NullWritable, Text> {
        String[] dictionaryVector;

        @Override
        public void setup(Context context) {
            String path = context.getConfiguration().get("org.sleuthkit.hadoop.dictionary");

            try {
                //FSDataInputStream in = fs.open(inFile);
                dictionaryVector = ClusterUtil.loadTermDictionary(context.getConfiguration(), FileSystem.get(context.getConfiguration()), path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(Text key, Cluster value, Context context)
        throws IOException {
            Vector v = value.getCenter();
            value.getNumPoints();
            
            JSONObject obj = new JSONObject();
            try {
                obj.put("a", key.toString());
                obj.put("n", String.valueOf(value.getNumPoints()));
                obj.put("kw", ClusterUtil.getTopFeatures(v, dictionaryVector, 10));
            } catch (JSONException ex) {
                ex.printStackTrace();
            }

            try {
                context.write(NullWritable.get(), new Text(obj.toString()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    // There should be no duplicates, but we'll use the set reducer anyways
    // as it is convenient and already written.
    public static class SetReducer extends Reducer<IntWritable, Text, IntWritable, ArrayWritable>{
        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                Context ctx)
        throws IOException {

            Set<String> s = new HashSet<String>();

            for (Text t : values) {
                s.add(t.toString());
            }
            String[] str = new String[1];
            try {
                ctx.write(key, new ArrayWritable(s.toArray(str)));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}