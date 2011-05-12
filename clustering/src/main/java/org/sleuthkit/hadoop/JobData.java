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
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

public class JobData {
    public static void main (String[] argv) throws IOException {
        System.out.println("Printing out Names:");
        Configuration  conf = new Configuration();
        JobClient jobClient = new JobClient(new InetSocketAddress("localhost",8021),conf);
        jobClient.setConf(conf); // Bug in constructor, doesn't set conf.

         for(JobStatus js: jobClient.getAllJobs()){
            // We only care about completed jobs.
             if(!js.isJobComplete()){
                 continue;
             } 
             RunningJob rj = jobClient.getJob(js.getJobID());
             if (rj != null && rj.getJobName() != null) {
                 System.out.println(rj.getJobName());
             } else {
                 System.out.println("NULL JOB OR NAME");
             }
         }
         System.out.println("Done");
    }
}
