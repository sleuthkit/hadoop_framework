package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

public class ClusterDocuments {

    
    public static int runPipeline(String input, String output, String dictionary) {
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
            CanopyDriver.run(conf, canopyInputPath, canopyOutputPath, new CosineDistanceMeasure(), .8, .65, true, false);
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
            Job job = new Job();
            job.setJarByClass(ClusterDocuments.class);

            job.setJobName("TP$IMG_ID_NUMBER$CommonName$AssociateClusters");

            FileInputFormat.setInputPaths(job, new Path(output + "/kmeans/clusteredPoints/"));
            FileOutputFormat.setOutputPath(job, new Path(output + "/kmeans/reducedClusters/"));

            job.setMapperClass(ClusterDocuments.ClusterMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(ClusterDocuments.SetReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(ArrayWritable.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.waitForCompletion(true);
            
            ////////////////////////////////
            // Output top cluster matches //
            ////////////////////////////////
            job = new Job();
            job.setJarByClass(ClusterDocuments.class);
            
            job.setJobName("TP$IMG_ID_NUMBER$CommonName$TopClusterWords");
            
            
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

            // FIXME: This may need to be changed, as it grabs the first set of clusters.
            // If there are many kmeans iterations, this may be sub-optimal.
            FileInputFormat.setInputPaths(job, goodPath);
            FileOutputFormat.setOutputPath(job, new Path(output + "/kmeans/topClusters/"));
            
            job.setMapperClass(ClusterDocuments.TopPointsMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            
            job.getConfiguration().set("org.sleuthkit.hadoop.dictionary", dictionary);
            return job.waitForCompletion(true) ? 0 : 1;
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
    
    // Generates text listing the top 10 features of the vectors we are
    // using as centroids for our clusters.
    public static class TopPointsMapper
    extends Mapper<Text, Cluster, Text, Text> {
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
            String output = String.valueOf(value.getNumPoints());
            output = output.concat(ClusterUtil.getTopFeatures(v, dictionaryVector, 10));
            try {
                context.write(key, new Text(output));
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