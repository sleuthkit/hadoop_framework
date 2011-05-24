package org.sleuthkit.web.sampleapp.server;

public class CommandThread extends Thread {

	String tCommandPattern;
	String tFileName;
	String tId;
	Process process = null;
	
	public CommandThread(String tCommandPattern, String tFileName, String tId) {
		super();
		this.tCommandPattern = tCommandPattern;
		this.tFileName = tFileName;
		this.tId = tId;
		this.setPriority(Thread.MIN_PRIORITY);
	}

	@Override
	public void run() {
		String commandPattern = this.tCommandPattern;
		String fileName = this.tFileName;
		String id = this.tId;
						
		String command = commandPattern.replace("$file", fileName).replace("$id", id);
		
	    try {
	      process = Runtime.getRuntime().exec(command);
	    }
	    catch (Throwable t) {
	      String msg = t.getMessage();
	      if( t.getCause() != null) {
	    	  msg += ", caused by " + t.getCause().getMessage();
	      }
	      System.out.println("Problem excecuting: " + msg);
	    }	
	}

	public void doStop() {
		if( process != null) {
	      process.destroy();
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		doStop();
		super.finalize();
	}
}
