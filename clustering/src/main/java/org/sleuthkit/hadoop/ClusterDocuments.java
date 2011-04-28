package org.sleuthkit.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.CosineDistanceMeasure;

public class ClusterDocuments {

    
    public static int runPipeline(String input, String output) {
        Configuration conf = new Configuration();
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
            CanopyDriver.run(conf, canopyInputPath, canopyOutputPath, new CosineDistanceMeasure(), .65, .95, true, false);
        } catch (Exception e) {
            return 1;
        }
        
        try {
            KMeansDriver.run(conf, kmeansInputPath, kmeansClusters, kmeansOutputPath, new CosineDistanceMeasure(), .5, 20, true, false);
        } catch (Exception e) {
            return 2;
        }
        
        return 0;
    }
}