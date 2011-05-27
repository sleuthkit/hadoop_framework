package org.sleuthkit.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

public class SKJobFactory {
    
    public static Job createJob(String imageID, String friendlyName, String step) throws IOException {
        Job j = new Job();
        
        j.setJobName("TP$" + imageID + "$" + friendlyName + "$" + step);
        j.getConfiguration().set(SKMapper.ID_KEY, imageID);
        j.getConfiguration().set(SKMapper.USER_ID_KEY, friendlyName);

        return j;
    }

}
