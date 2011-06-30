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

package org.sleuthkit.web.sampleapp.server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.sleuthkit.web.sampleapp.client.SampleService;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;

/**
 * This class implements server side RPC service (see SampleService interface).
 */
@SuppressWarnings("serial")
public class SampleServiceImpl extends RemoteServiceServlet implements SampleService {
	
	/** job names in hadoop  order of analysis progress (first job - before it appears in hadoop) */
	private static String jobStepNames[] = {"UploadingIntoHadoop", "TikaTextExtraction", "GrepSearch", "GrepCountJson", "GrepMatchJson", "GrepMatchesToSequenceFiles", "TopClusterMatchPrinting", 
		"ClusteredVectorsToJson", "CrossImageSimilarityScoring", "CrossImageScoreCalculation"};
	
	static ResourceBundle rb = null;
	/** properties defined in resource file */
	static String filesDirPath = null;
	static String commandScript = null;
	static String commandJar = null;
	static String[] columns = null;
	static String reportPattern = null;
	static String reportWS = null;
	static String path = null;
	static String fsripLib = null;
	static String hadoopHome = null;
	static String workDir = null;
	static String DATE_FORMAT = "yyyy/MM/dd HH:mm:ss";
//	static String reportLocalPattern = null;

	/** data columns (enums not used here because it would be too verbose) */
	static int JOB_NAME = 0;
	static int IMAGE_ID = 1;
	static int CUR_TASK = 2;
	static int TASK_STATUS = 3;
	static int TIME_START = 4;
	static int MAP_PROGR = 5;
	static int REDUCE_PROGR = 6;
	
	/** map of job id to time job was started */
	static Map<String, Date> imagesSubmitted = new HashMap<String, Date>();
	
    /**
     * read properties from property file
     */
	static void readProps() {
		// Read the config parameters from properties file
		try {
			rb = ResourceBundle.getBundle("sampleapp");
			filesDirPath = rb.getString("files.dir.path");
			commandScript = rb.getString("command.script");
			commandJar = rb.getString("command.jar");
			reportPattern = rb.getString("report.hdfs.pattern");
			reportWS = rb.getString("report.ws.pattern");
			path = rb.getString("command.path");
			fsripLib = rb.getString("command.fsrip.lib");
			hadoopHome = rb.getString("command.hadoop.home");
			workDir = rb.getString("command.work_dir");
			
			String cols = rb.getString("columns");
			StringTokenizer st = new StringTokenizer(cols, ",");
			columns = new String[st.countTokens()];
			int i=0;
			while (st.hasMoreTokens()) {
				columns[i++] = st.nextToken();
			}			
		} 
		catch (Exception ex) {
			System.err.println("Error initializing application, cannot read configuration.");
			ex.printStackTrace(System.err);
		}
	}
	
	static String getFilesDirPath() {
		if( filesDirPath == null) {
			readProps();
		}
		return filesDirPath;
	}
	
    /**
     * start a new process by executing a command script with arguments
     * @param fileName path to image file
     * @param id job name
     * @return command executed with all arguments
     * @throws Exception
     */
	public String runAsync(String fileName, String id) throws IllegalArgumentException {		
		if (!imagesSubmitted.containsKey(id)) {
			imagesSubmitted.put(id, Calendar.getInstance().getTime());
		}
	    try {
	    	 ProcessBuilder pb = new ProcessBuilder(commandScript, id, fileName, commandJar);
	    	 pb.directory(new File(workDir));
	    	 Map<String, String> env = pb.environment();
	    	 
	    	 env.put("LD_LIBRARY_PATH", SampleServiceImpl.fsripLib);
	    	 env.put("HADOOP_HOME", SampleServiceImpl.hadoopHome);
	    	 env.put("PATH", env.get("PATH") + SampleServiceImpl.path);

	    	 pb.start();
	    }
	    catch (Throwable t) {
			t.printStackTrace();
			throw new IllegalArgumentException(t.getMessage());
	    }			
	    String command = commandScript + " " + id + " " + fileName + " " + commandJar;
		System.err.println("Process started: " + command);
		return command;
	}
	
    /**
     * get job data (one row per job) from hadoop
     * @return array of jobs (one row per job). Each row is an array of strings (one per column), matching columns[].
     * @throws Exception
     */
	public String[][] getData() throws IllegalArgumentException {

		List<String[]> result = new ArrayList<String[]>();
		//first we add "ghost jobs" based on image ids we have - update these if we have real jobs
		addGhostJobs(result);
		Configuration conf = new Configuration();
		JobClient jobClient;
		
		try {
			jobClient = new JobClient(new InetSocketAddress("localhost", 8021), conf);
			jobClient.setConf(conf); // Bug in constructor, doesn't set conf.
	
			for (JobStatus js : jobClient.getAllJobs()) {
				RunningJob rj = jobClient.getJob(js.getJobID());
				if (rj == null) 
					continue;
				String jobName = rj.getJobName();
				if (jobName == null)
					continue;
				//extract TP$imageHash$imageId$step from jobName - we filter on image hash
				if (jobName.startsWith("TP") && !jobName.contains("_TEST")) {
					String[]names = jobName.split("\\$");
					if (names.length != 4) {
						System.err.println("Invalid job name of TP job " + jobName);
					}
					processJob(result, names, js, rj);
				}
			}	
			//sort descending by time
	        Collections.sort(result, new Comparator<String[]>(){	        	 
	            public int compare(String[] a1, String[] a2) {
	               return a2[TIME_START].compareTo(a1[TIME_START]);
	            }	 
	        });
			return result.toArray(new String[0][0]);
		} 
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
    /**
     * add job data into a list of jobs for jobs that have been submitted, but not yet in hadoop
     * @param tableOfJobs list of jobs
     */
	private void addGhostJobs(List<String[]> tableOfJobs) {
		Set<Map.Entry<String, Date>> entrySet = imagesSubmitted.entrySet();
		Iterator<Map.Entry<String, Date>> it = entrySet.iterator();
		
		while (it.hasNext()) {			
			String[] colData = new String[columns.length];
			Map.Entry<String, Date> entry = it.next();
			colData[JOB_NAME] = entry.getKey();
			colData[IMAGE_ID] = "";
			colData[CUR_TASK] = jobStepNames[0];
			colData[TASK_STATUS] = "ingest";
			colData[TIME_START] = new SimpleDateFormat(DATE_FORMAT).format(entry.getValue());	
			colData[MAP_PROGR] = "0%";
			colData[REDUCE_PROGR] = "0%";
			
			tableOfJobs.add(colData);
		}		
	}
	
    /**
     * process a hadoop job - maintain only "highest order" job info for each image
     * @param tableOfJobs job list
     * @param parsedJobName job name from hadoop parsed into 4 parts based on naming convention "TP$imageHash$imageId$step"
     * @param js hadoop job status
     * @param rj hadoop running job
     * @throws Exception
     */
	private void processJob(List<String[]> tableOfJobs, String[] parsedJobName, JobStatus js, RunningJob rj) throws IOException {

		String[]row = findOrCreateJobRow(tableOfJobs, parsedJobName[1], parsedJobName[2]);
		int jobInd = (row[IMAGE_ID] == null) ? -1 : getJobIndex(row[IMAGE_ID]);
		int thisJobInd = getJobIndex(parsedJobName[3]);

		//need to update the same job index too to report progress
		row[IMAGE_ID] = parsedJobName[1];	//hash
		row[JOB_NAME] = parsedJobName[2];	//id

		if (thisJobInd < 0) {
			//this should not happen: report error and continue
			System.err.println("Unknown job step " + parsedJobName[3] + " for image hash " + parsedJobName[1]);
			row[CUR_TASK] = parsedJobName[3];	//state
			row[TASK_STATUS] = getSimpleJobStatus(rj.getJobState());	//status
			setRowData(row, js);
		}
		else if (thisJobInd >= jobInd) {
			row[CUR_TASK] = parsedJobName[3];	//state
			row[TASK_STATUS] = getJobStatus(jobStepNames[jobStepNames.length-1].compareTo(parsedJobName[3]) == 0, rj.getJobState(), parsedJobName[1]);	//status
			setRowData(row, js);
		}
	}
	
    /**
     * set data for some columns of one row
     * @param row row data
     * @param js hadoop job status
     */
	private void setRowData(String[]row, JobStatus js) {
		row[TIME_START] = new SimpleDateFormat(DATE_FORMAT).format(new Date(js.getStartTime()));			 
		row[MAP_PROGR] = Integer.toString((int)(js.mapProgress()*100)) + "%";
		row[REDUCE_PROGR] = Integer.toString((int)(js.reduceProgress()*100)) + "%";
	}
	
    /**
     * find a job row or create a new one
     * @param tableOfJobs job list
     * @param imageId
     * @param jobName
     * @return job row
     */
	private String[] findOrCreateJobRow(List<String[]> tableOfJobs, String imageId, String jobName) {
		String[] colData = null;
		Iterator<String[]> it = tableOfJobs.iterator();
		while(it.hasNext()) {
			colData = it.next();
			if ((colData[IMAGE_ID].compareTo(imageId) == 0) || (colData[IMAGE_ID].length() == 0 && jobName.compareTo(colData[JOB_NAME]) == 0))
				return colData;
		}
		//create a new row for this image id
		colData = new String[columns.length];
		tableOfJobs.add(colData);
		return colData;
	}
	
    /**
     * get job index into job names array
     * @param jobStep
     * @return job index (or -1, if not found)
     */
	private int getJobIndex(String jobStep) {
		for (int i = 0; i < jobStepNames.length; i++) {
			if (jobStepNames[i].compareTo(jobStep) == 0)
				return i;
		}
		return (-1);
	}
	
    /**
     * get job status
     * @param isFinalStep is this a final job step?
     * @param status hadoop job status
     * @param imageHash
     * @return job status
     * @throws Exception
     */
	private String getJobStatus(boolean isFinalStep, int status, String imageHash) throws IOException {
		if (isFinalStep && status == JobStatus.SUCCEEDED) {
			return makeReportLink(imageHash);
		}
		return getSimpleJobStatus(status);
	}
	
    /**
     * get simple job status
     * @param status hadoop job status
     * @return job status
     * @throws Exception
     */
	private String getSimpleJobStatus(int status) throws IOException {
		switch (status) {		
			case JobStatus.FAILED: return "failed";
			case JobStatus.KILLED: return "killed";
			case JobStatus.PREP: 	return "prepared";
			case JobStatus.RUNNING: return "running";
			case JobStatus.SUCCEEDED: return "completed";
			
			default: return "invalid";
		}
	}
	
    /**
     * create a report HTML link
     * @param imageHash
     * @return report HTML link
     * @throws Exception
     */
	private String makeReportLink(String imageHash) throws IOException {
		String hdfsPath = reportPattern.replace("$hash", imageHash).replace("/", "%2F");
		return ("<a href=\"" + reportWS.replace("$file", hdfsPath) + "\">Report</a>");
	}

	/** copy file from HDFS to local FS - Not used any more
	private String getLocalFile(String imageHash) throws IOException {
		Configuration conf = new Configuration();
		String hdfsPath = reportPattern.replace("$hash", imageHash);
		String localPath = reportLocalPattern.replace("$hash", imageHash);

		File localFile = new File(localPath);
		if (!localFile.exists()) {	//don't copy report if it's already there
			FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
			FileUtil.copy(fs, new Path(hdfsPath), localFile, false, conf);
		}
		System.err.println("created link to: " + localFile.getPath());
		return localFile.getPath();
	}
	**/

    /**
     * get array of column names
     * @return array of column names
     * @throws Exception
     */
	public String[] getColNames() throws IllegalArgumentException {		
		return columns;
	}
		
    /**
     * get a list of files in a directory
     * @param dirPath directory path
     * @return list of files in a directory
     * @throws Exception
     */
	public List<String> getFiles(String dirPath) throws IllegalArgumentException {
		
		Set<String> sortedFiles = new TreeSet<String>();
		List<String> files = new Vector<String>();
		
		// Retrieve the files in configured directory
		File dir = new File(dirPath);
		
		for(File file : dir.listFiles()) {
			if ( file.isDirectory()) {
				sortedFiles.add("+" + file.getAbsolutePath());
			} 
			else {
				sortedFiles.add(file.getAbsolutePath());
			}
		}
		if ( dir.getAbsolutePath().lastIndexOf("/") >= 0) {
			String parentPath = dir.getAbsolutePath().substring(0, dir.getAbsolutePath().lastIndexOf("/"));
			if ( parentPath.equals("")) {
				parentPath = "/";
			}
			//can't go to other branches
			if (parentPath.startsWith(filesDirPath))
				files.add("+" + parentPath);
			//don't need this any more
			//files.add("+" + dir.getAbsolutePath());
		}
		Iterator<String> it = sortedFiles.iterator();
		while (it.hasNext()) {
			files.add(it.next());
		}
		return files;
	}

    /**
     * get a list of configuration parameters
     * @return list of configuration parameters
     */
	public List<String> getConfigParams() {
		List<String> retVal = new Vector<String>();
		retVal.add(getFilesDirPath());
		return retVal;
	}
}
