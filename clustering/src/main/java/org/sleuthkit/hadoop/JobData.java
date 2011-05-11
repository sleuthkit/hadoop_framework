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
