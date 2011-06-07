package org.sleuthkit.hadoop;

public class JobNames {

    // Text extraction. This is the first thing that happens in the pipeline.
    public static final String TIKA_TEXT_EXTRACTION = "TikaTextExtraction";

    // The grep search proper.
    public static final String GREP_SEARCH = "GrepSearch";
    
    // Grep reporting - these things happen after we perform the grep search.
    public static final String GREP_COUNT_MATCHED_EXPRS_JSON = "GrepCountJson";
    public static final String GREP_MATCHED_EXPRS_JSON =  "GrepMatchJson";

    // Cluster prep - these happen after the grep search, as we only cluster files
    // with keyword hits.
    public static final String GREP_MATCHED_FILES_OUT = "GrepMatchesToSequenceFiles";
    public static final String OUTPUT_CLUSTER_MATCH = "TopClusterMatchPrinting";
    
    // Cluster reporting - these things happen last.
    public static final String CROSS_IMG_SIM_SCORING = "CrossImageSimilarityScoring";
    public static final String CROSS_IMG_SIM_SCORING_CALC = "CrossImageScoreCalculation";
}
