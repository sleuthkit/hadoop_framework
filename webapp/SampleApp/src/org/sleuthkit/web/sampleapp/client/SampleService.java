package org.sleuthkit.web.sampleapp.client;


import java.util.List;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("sample")
public interface SampleService extends RemoteService {
	String runAsync(String commandPattern, String fileName, String id) throws IllegalArgumentException;
	String[][] getData() throws IllegalArgumentException;
	String[] getColNames() throws IllegalArgumentException;
	String run(String commandPattern, String fileName, String id) throws IllegalArgumentException;
	List<String> getFiles(String currentDir) throws IllegalArgumentException;
	List<String> getConfigParams();
}
