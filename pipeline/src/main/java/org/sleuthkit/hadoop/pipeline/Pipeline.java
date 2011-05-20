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


package org.sleuthkit.hadoop.pipeline;

import org.sleuthkit.hadoop.ClusterDocuments;
import org.sleuthkit.hadoop.GrepSearchJob;
import org.sleuthkit.hadoop.HBaseFileImporter;
import org.sleuthkit.hadoop.TikaTextExtractor;
import org.sleuthkit.hadoop.TokenizeAndVectorizeDocuments;

public class Pipeline {
/*
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
    public static final String VECTOR_DIR = "hdfs://localhost/texaspete/vectors";
    public static final String TFIDF_DIR = "hdfs://localhost/texaspete/vectors/tfidf-vectors/";

    public static final String CLUSTERS_DIR = "hdfs://localhost/texaspete/clusters";

    public static final String DICTIONARY_DIR = "hdfs://localhost/texaspete/vectors/dictionary.file-0";

    public static void main(String[] argv) throws Exception {
        // TODO: Run the TIKA code here, collecting content from text files.


        HBaseFileImporter.runPipeline(argv[0]);

        TikaTextExtractor.runPipeline("fileTable", "data:path", "data:cont", "hdfs://localhost/texaspete/text/outfile");

        // Run the GREP search code.

        GrepSearchJob.runPipeline(TIKA_OUT_DIR, GREP_OUT_DIR, GREP_KEYWORDS);

        TokenizeAndVectorizeDocuments.runPipeline(TIKA_OUT_DIR, TOKEN_OUT_DIR, VECTOR_DIR);

        ClusterDocuments.runPipeline(TFIDF_DIR, CLUSTERS_DIR, DICTIONARY_DIR);
    }
*/
}
