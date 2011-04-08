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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansDriver;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VectorAndClusterDocuments extends AbstractJob {
    
    private static final Logger log = LoggerFactory.getLogger(VectorAndClusterDocuments.class);

    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data";
    
    // this is just a placeholder, since raw text processing should occur in
    // a separate component.
    private static final String DIR_PLAINTEXT = "/texaspete/raw";
    
    
    private static final String DIR_TEXT_SEQFILE = "hdfs://localhost/texaspete/text";
    private static final String DIR_TOKENIZED_SEQFILE = "hdfs://localhost/texaspete/tokens";
    private static final String DIR_VECTOR_SEQFILE = "hdfs://localhost/texaspete/vectors";
    
    
    private static final String M_OPTION = FuzzyKMeansDriver.M_OPTION;

    private VectorAndClusterDocuments() {
    }
    
    public static void main (String[] argv) throws IOException, ClassNotFoundException, InterruptedException, InstantiationException, IllegalAccessException {
        new VectorAndClusterDocuments().run();
    }

    @Override
    public int run(String[] argv) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }
    
    public int run() throws IOException, ClassNotFoundException, InterruptedException, InstantiationException, IllegalAccessException {
        // Placeholder. This will convert a sample file into the sequencefile we so desire.
        Path input;
        Path output;
        
        // Assume we already have a ID:Text sequencefile directory.
        // We now proceed to run the DocumentProcessor class, which will turn
        // the SequenceFile into a ID:StringTuple file.
        input = new Path(DIR_TEXT_SEQFILE);
        output = new Path(DIR_TOKENIZED_SEQFILE);
        
        //StandardAnalyzer sta = new StandardAnalyzer(Version.LUCENE_30);
        
        DocumentProcessor.tokenizeDocuments(input, DefaultAnalyzer.class, output);
        
        // We are now going to take the SequenceFile we got from above and
        // convert it to vectors using DictionaryVectorizer. This should create
        // a SequenceFile of ID:Vector. This is the final processing step we
        // need to do; after this we have our vector list to work off of.
        input = output;
        output = new Path(DIR_VECTOR_SEQFILE);
        
        int minSupport = 2;
        int maxNGramSize = 1;
        float minLLRValue = 1;
        float normPower = 1;
        boolean logNormalize = false;
        int numReducers = 2;
        int chunkSizeInMegabytes = 200;
        boolean sequentialAccess = false;
        boolean namedVectors = true;
        
        DictionaryVectorizer.createTermFrequencyVectors(input, 
                output,
                getConf(),
                minSupport,
                maxNGramSize,
                minLLRValue,
                normPower,
                logNormalize,
                numReducers,
                chunkSizeInMegabytes,
                sequentialAccess,
                namedVectors);
        
        // Cluster.
        // Generally how this seems to work from the examples that I've found
        // is that one does a CanopyCluster first, which is a fast cluster
        // algorithm for which we don't need to pick starting clusters. The
        // centroids for the canopies are then used as the starting points in
        // k-means or fuzzy-k-means clustering.
        
        
//        
//        DistanceMeasure measure = new EuclideanDistanceMeasure();
//        double t1 = 80;
//        double t2 = 55;
//        double convergenceDelta = 0.5;
//        int maxIterations = 10;
//        float fuzziness = 2;
//        Path directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
//        log.info("Preparing Input");
//        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
//        log.info("Running Canopy to get initial clusters");
//        CanopyDriver.run(new Configuration(), directoryContainingConvertedInput, output, measure, t1, t2, false, false);
//        log.info("Running FuzzyKMeans");
//        FuzzyKMeansDriver.run(directoryContainingConvertedInput,
//                              new Path(output, Cluster.INITIAL_CLUSTERS_DIR),
//                              output,
//                              measure,
//                              convergenceDelta,
//                              maxIterations,
//                              fuzziness,
//                              true,
//                              true,
//                              0.0,
//                              false);
//        // run ClusterDumper
//        ClusterDumper clusterDumper =
//            new ClusterDumper(finalClusterPath(getConf(), output, maxIterations), new Path(output, "clusteredPoints"));
//        clusterDumper.printClusters(null);
        return 0;
    }
    
    /**
     * Return the path to the final iteration's clusters
     */
    private static Path finalClusterPath(Configuration conf, Path output, int maxIterations) throws IOException {
      FileSystem fs = FileSystem.get(conf);
      for (int i = maxIterations; i >= 0; i--) {
        Path clusters = new Path(output, "clusters-" + i);
        if (fs.exists(clusters)) {
          return clusters;
        }
      }
      return null;
    }
    
    
}

