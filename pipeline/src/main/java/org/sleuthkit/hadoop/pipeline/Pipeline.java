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
import org.sleuthkit.hadoop.FSEntryTikaTextExtractor;
import org.sleuthkit.hadoop.GrepReportGenerator;
import org.sleuthkit.hadoop.GrepSearchJob;
import org.sleuthkit.hadoop.SequenceFsEntryText;
import org.sleuthkit.hadoop.TokenizeAndVectorizeDocuments;

import com.lightboxtechnologies.spectrum.HBaseTables;

public class Pipeline {
    // A file containing lines of text, each of which represents a regex.
    public static final String GREP_KEYWORDS = "/texaspete/regexes";

    public static void main(String[] argv) throws Exception {
        String imageID = argv[0];
        String seqDumpDirectory = "/texaspete/data/" + imageID + "/text";
        String tokenDumpDirectory = "/texaspete/data/" + imageID + "/tokens";
        String vectorDumpDirectory = "/texaspete/data/" + imageID + "/vectors";
        String clusterDumpDirectory = "/texaspete/data/" + imageID + "/clusters";
        String dictionaryDumpDirectory = "/texaspete/data/" + imageID + "/vectors/dictionary.file-0";

        FSEntryTikaTextExtractor.runPipeline(HBaseTables.ENTRIES_TBL, imageID, "FriendlyName");
        GrepSearchJob.runPipeline(HBaseTables.ENTRIES_TBL, imageID, GREP_KEYWORDS, "FriendlyName");
        SequenceFsEntryText.runTask(seqDumpDirectory, imageID, "FriendlyName");

        TokenizeAndVectorizeDocuments.runPipeline(seqDumpDirectory, tokenDumpDirectory, vectorDumpDirectory);
        GrepReportGenerator.runPipeline(GREP_KEYWORDS, imageID, "FriendlyName");

        ClusterDocuments.runPipeline(vectorDumpDirectory + "/tfidf-vectors/", clusterDumpDirectory, dictionaryDumpDirectory, .65, .65, imageID, "FriendlyName");
    }

}
