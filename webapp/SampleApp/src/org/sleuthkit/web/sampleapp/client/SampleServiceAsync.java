package org.sleuthkit.web.sampleapp.client;

import java.util.List;

import com.google.gwt.user.client.rpc.AsyncCallback;

/**
 * The async counterpart of <code>GreetingService</code>.
 */
public interface SampleServiceAsync {
	void runAsync(String commandPattern, String fileName, String id,
			AsyncCallback<String> callback);
	void getData(AsyncCallback<String[][]> callback);
	void getColNames(AsyncCallback<String[]> callback);
	void run(String commandPattern, String fileName, String id, AsyncCallback<String> callback)
			throws IllegalArgumentException;
	void getFiles(String currentDir, AsyncCallback<List<String>> callback);
	void getConfigParams(AsyncCallback<List<String>> asyncCallback);
}
