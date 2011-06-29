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

/** Contains constants for Job names used when we generate and spin off
 * Hadoop jobs. */
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
    public static final String OUTPUT_CLUSTER_JSON = "ClusteredVectorsToJson";   
    
    // Cluster reporting - these things happen last.
    public static final String CROSS_IMG_SIM_SCORING = "CrossImageSimilarityScoring";
    public static final String CROSS_IMG_SIM_SCORING_CALC = "CrossImageScoreCalculation";
}
