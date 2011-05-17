package org.sleuthkit.web.sampleapp.server;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
 * The server side implementation of the RPC service.
 */
@SuppressWarnings("serial")
public class SampleServiceImpl extends RemoteServiceServlet implements SampleService {
	static ResourceBundle rb = null;
	static String filesDirPath = null;
	static String commandPattern = null;
	static String[] columns = null;
	
	CommandThread commandThread = null;
	
	static void readProps() {
		// Read the config parameters from properties file
		try {
			rb = ResourceBundle.getBundle("sampleapp");
			filesDirPath = rb.getString("files.dir.path");
			commandPattern = rb.getString("command.pattern");
			
			String cols = rb.getString("columns");
			StringTokenizer st = new StringTokenizer(cols, ",");
			columns = new String[st.countTokens()];
			int i=0;
			while(st.hasMoreTokens()) {
				columns[i++] = st.nextToken();
			}
			
		} catch(Exception ex) {
			System.err.println("Error initializing application, cannot read configuration. Using defaults");
			ex.printStackTrace(System.err);
			filesDirPath = "/tmp";
			commandPattern = "ls -l $file";
		}
	}
	
	static String getCommandPattern() {
		if( commandPattern == null) {
			readProps();
		}
		return commandPattern;
	}
	static String getFilesDirPath() {
		if( filesDirPath == null) {
			readProps();
		}
		return filesDirPath;
	}
	
	public String runAsync(String commandPattern, String fileName, String id) throws IllegalArgumentException {
		
		
		if( commandThread != null) {
			// Another previous command was running, stop now
			commandThread.doStop();
		}		
		commandThread = new CommandThread(commandPattern,
				escapeHtml(fileName),
				escapeHtml(id));
		commandThread.start();
		
		String commandString = commandPattern.replace("$file", fileName).replace("$id", id);
		return commandString;
	}
		
	public String[][] getData() throws IllegalArgumentException {
/* 	dummy impl
		String[][] retVal = {{"1", "2", "3"}, {"4", "5", "6"}, {"7", "8", "9"}};		
		// Randomly, it returns null (10% probability)
		if( Math.random() < 0.1) {
			retVal = null;
		}		
		return retVal;
*/	
		List<String[]> result = new ArrayList<String[]>();
		Configuration conf = new Configuration();
		//JobClient jobClient = new JobClient(new InetSocketAddress("localhost", 8021), conf);
		JobClient jobClient = new JobClient(new InetSocketAddress("hercules", 8021), conf);
		jobClient.setConf(conf); // Bug in constructor, doesn't set conf.

		for (JobStatus js : jobClient.getAllJobs()) {
			RunningJob rj = jobClient.getJob(js.getJobID());
			if (rj == null) 
				continue;
			String jobName = rj.getJobName();
			if (jobName == null)
				continue;
			
			String[]colData = new String[columns.length];
			colData[0] = jobName;
			colData[1] = getJobStatus(rj.getJobState());
			colData[2] = Integer.toString(((int)js.mapProgress())*100);
			colData[3] = Integer.toString(((int)js.reduceProgress())*100);
			
			result.add(colData);
		}	
		return (String[][]) result.toArray();
	}
	private String getJobStatus(int status) {
		switch (status) {
		
		case JobStatus.FAILED: return "failed";
		case JobStatus.KILLED: return "killed";
		case JobStatus.PREP: 	return "prep";
		case JobStatus.RUNNING: return "running";
		case JobStatus.SUCCEEDED: return "succeeded";
		
		default: return "invalid";
		}
	}

	public String[] getColNames() throws IllegalArgumentException {		
		return columns;
	}
	
	@SuppressWarnings("deprecation")
	public String run(String commandPattern, String fileName, String id) throws IllegalArgumentException {
		// Escape data from the client to avoid cross-site script vulnerabilities.
		fileName = escapeHtml(fileName);
		id = escapeHtml(id);
		
		StringWriter output = new StringWriter();
		
		String command = commandPattern.replace("$file", fileName).replace("$id", id);
		
		Runtime runtime = Runtime.getRuntime();
	    Process process = null;
	    try {
	      output.append("> " + command + "<br/><br/>");
	      process = runtime.exec(command);
	      DataInputStream in = new DataInputStream(process.getInputStream());

	      // Read and print the output
	      String line = null;
	      while ((line = in.readLine()) != null) {
	        output.append(line + "<br/>");
	      }
	    }
	    catch (Throwable t) {
	      String msg = t.getMessage();
	      if( t.getCause() != null) {
	    	  msg += ", caused by " + t.getCause().getMessage();
	      }
	      output.append("Problem excecuting: " + msg);
	    }
		
		return output.toString();
	}
	
	@Override
	public List<String> getFiles(String currentDir)
			throws IllegalArgumentException {
		Set<String> sortedFiles = new TreeSet<String>();
		List<String> files = new Vector<String>();
		
		// Retrieve the files in configured directory

		File dir = new File(currentDir);
		
		for(File file : dir.listFiles()) {
			if( file.isDirectory()) {
				sortedFiles.add("+" + file.getAbsolutePath());
			} else {
				sortedFiles.add(file.getAbsolutePath());
			}
		}

		if( dir.getAbsolutePath().lastIndexOf("/") >= 0) {
			String parentPath = dir.getAbsolutePath().substring(0, dir.getAbsolutePath().lastIndexOf("/"));
			if( parentPath.equals("")) {
				parentPath = "/";
			}
			files.add("+" + parentPath);
			files.add("+" + dir.getAbsolutePath());
		}
		Iterator<String> it = sortedFiles.iterator();
		while(it.hasNext()) {
			files.add(it.next());
		}
		return files;
	}

	@Override
	public List<String> getConfigParams() {
		List<String> retVal = new Vector<String>();
		retVal.add(getCommandPattern());
		retVal.add(getFilesDirPath());
		return retVal;
	}
	
	/**
	 * Escape an html string. Escaping data received from the client helps to
	 * prevent cross-site script vulnerabilities.
	 * 
	 * @param html the html string to escape
	 * @return the escaped string
	 */
	private String escapeHtml(String html) {
		if (html == null) {
			return null;
		}
		return html.replaceAll("&", "&amp;").replaceAll("<", "&lt;")
				.replaceAll(">", "&gt;");
	}

}
