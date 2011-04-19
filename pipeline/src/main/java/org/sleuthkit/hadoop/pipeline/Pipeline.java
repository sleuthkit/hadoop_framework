package org.sleuthkit.hadoop.pipeline;

import org.sleuthkit.hadoop.GrepSearchJob;
import org.sleuthkit.hadoop.HBaseFileImporter;
import org.sleuthkit.hadoop.TikaTextExtractor;
import org.sleuthkit.hadoop.TokenizeAndVectorizeDocuments;

public class Pipeline {
    
    // The storage place of raw files containing:
    
    // content: raw file content bytes
    public static final String TIKA_IN  = "hdfs://localhost/texaspete/raw/";
    
    // The storage place of sequencefiles containing:
    // key:   file name
    // value: file content (plain text)
    public static final String TIKA_OUT_DIR = "hdfs://localhost/texaspete/text/";

    // The storage place of sequencefiles containing:
    // key:   file name
    // value: keyword vector (should be a set)
    public static final String GREP_OUT_DIR = "hdfs://localhost/texaspete/grepped/";
    
    // A file containing lines of text, each of which represents a regex.
    public static final String GREP_KEYWORDS = "hdfs://localhost/texaspete/regexes";
    
    // The storage place of sequencefiles containing:
    // key:   file name
    // value: tokenized file content (vector of words)
    public static final String TOKEN_OUT_DIR = "hdfs://localhost/texaspete/tokens";
    
    // The storage place of sequencefiles containing:
    // key:   file name
    // value: term frequency vectors.
    // This directory will likely also contain a dictionary vector mapping
    // the indices in the sparse TF vectors to words.
    public static final String TF_DIR = "hdfs://localhost/texaspete/vectors";
    
    public static void main(String[] argv) throws Exception {
        // TODO: Run the TIKA code here, collecting content from text files.
        
        
        HBaseFileImporter.runPipeline(argv[0]);
        
        TikaTextExtractor.runPipeline("fileTable", "data:path", "data:cont", "hdfs://localhost/texaspete/text/outfile");
        
        // Run the GREP search code.

        GrepSearchJob.runPipeline(TIKA_OUT_DIR, GREP_OUT_DIR, GREP_KEYWORDS);
        
        TokenizeAndVectorizeDocuments.runPipeline(TIKA_OUT_DIR, TOKEN_OUT_DIR, TF_DIR);
        
        
    }
}
