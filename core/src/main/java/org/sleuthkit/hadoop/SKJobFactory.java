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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.filecache.DistributedCache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Factory method for creating jobs conforming to a naming convention
 * for use with the web interface.
 * 
 */
public class SKJobFactory {

  private static final Log LOG = LogFactory.getLog(SKJobFactory.class);
    
  public static void addDependencies(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] jars = fs.globStatus(new Path("/texaspete/lib/*.jar"));
    if (jars.length > 0) {
      for (FileStatus jar: jars) {
        LOG.info("Adding jar to DC/CP: " + jar.getPath());
        DistributedCache.addFileToClassPath(jar.getPath(), conf, fs);
      }
    }
    else {
      LOG.warn("Did not add any jars to distributed cache. This job will probably throw a ClassNotFound exception.");
    }
  }

  public static Job createJobFromConf(String imageID, String friendlyName, String step, Configuration conf) throws IOException {
    Job j = conf == null ? new Job(): new Job(conf);
    
    StringBuilder sb = new StringBuilder("TP$");
    sb.append(imageID);
    sb.append("$");
    sb.append(friendlyName);
    sb.append("$");
    sb.append(step);
    String jobname = sb.toString();
    j.setJobName(jobname);
    LOG.info("Configuring job " + jobname);
    j.getConfiguration().set(SKMapper.ID_KEY, imageID);
    j.getConfiguration().set(SKMapper.USER_ID_KEY, friendlyName);
    addDependencies(j.getConfiguration());
    return j;
  }
  
  public static Job createJob(String imageID, String friendlyName, String step) throws IOException {
    return createJobFromConf(imageID, friendlyName, step, null);
  }
}
