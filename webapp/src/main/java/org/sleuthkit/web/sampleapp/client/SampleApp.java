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

package org.sleuthkit.web.sampleapp.client;

import java.util.List;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.KeyCodes;
import com.google.gwt.event.dom.client.KeyUpEvent;
import com.google.gwt.event.dom.client.KeyUpHandler;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.FlexTable.FlexCellFormatter;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.PasswordTextBox;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.ListBox;

/**
 * This class (in conjunction with 3 JSP) implements GWT-based GUI for the TP web application. There are 3 "pages": login, main page and job monitor page.
 */
public class SampleApp implements EntryPoint {
	public static final String SERVER_ERROR = "A server error occurred: ";	
	public static final String RUN_WAIT_MSG = "Executing...";	
	public static final String ERROR_GETTING_CONFIG = "Error retrieving configuration parameters!";
	public static final String ERROR_GETTING_FILES = "Error retrieving files!";
	public static final String ERROR_ID_REQUIRED = "Image id is required";
		
	public static final String NO_ACCESS_MSG = "Access restricted!";
	public static final String NO_ACCESS = "NO_NO";	//set a path here to restrict access
	
	public static final int UPDATE_INTERVAL = 20;	//in sec
	public static final String SUBMIT_BUTTON_LABEL = "Analyze disk image"; 
	public static final String MONITOR_BUTTON_LABEL = "Monitor jobs"; 
	public static final String JOB_PAGE_BUTTON_LABEL = "Analyze another disk image"; 
		
	/**
	 * Create a remote service proxy to talk to the server-side service.
	 */
	private final SampleServiceAsync sampleService = GWT.create(SampleService.class);
	
	final Button submitButton = new Button(SUBMIT_BUTTON_LABEL);
	final Button monitorButton = new Button(MONITOR_BUTTON_LABEL);
	final Button goHomeButton = new Button(JOB_PAGE_BUTTON_LABEL);
	
	private final ListBox fileField = new ListBox();
	final FlexTable flexTable = new FlexTable();
	String[] colNames = null;
	
	Timer timer = new Timer() {
		public void run() {
			flexTable.removeAllRows();
			sampleService.getColNames(new ColNamesAsyncCallback<String[]>());
			sampleService.getData(new GetDataAsyncCallback<String[][]>());
		}
	};
	 
	/**
	 * This is the entry point method.
	 */
	public void onModuleLoad() {
		
		fileField.setVisibleItemCount(15);

		// Retrieve config parameters from server
		getConfigParams();
		
		// Create a handler to navigate
		class FileSelectHandler implements ClickHandler {
			@Override
			public void onClick(ClickEvent event) {
				if(fileField.getItemText(fileField.getSelectedIndex()).startsWith("+")) {
					populateFileField(fileField.getItemText(fileField.getSelectedIndex()));
					submitButton.setEnabled(false);
				} else {
					submitButton.setEnabled(true);					
				}
			}
		}
		FileSelectHandler fileSelectHandler = new FileSelectHandler();
		fileField.addClickHandler(fileSelectHandler);

		final TextBox idField = new TextBox();
		final HTML responseField = new HTML();
		
	    final FlexCellFormatter cellFormatter = flexTable.getFlexCellFormatter();

		final TextBox usernameField = new TextBox();
		final PasswordTextBox passwordField = new PasswordTextBox();
		final Button loginButton = new Button("Login");
		final ScrollPanel scrollPanel = new ScrollPanel();

		submitButton.addStyleName("sendButton");
		submitButton.setEnabled(false);
		monitorButton.setEnabled(true);		
		goHomeButton.setEnabled(true);
		
		idField.setWidth("400px");
		responseField.setWidth("640px");
		responseField.setVisible(false);

		scrollPanel.add(flexTable);
		scrollPanel.setSize("800px", "600px");
		scrollPanel.addStyleName("cw-ScollPanel");
		
	    flexTable.addStyleName("cw-FlexTable");
	    flexTable.setCellSpacing(2);
	    flexTable.setCellPadding(3);
		
		// Add the fields to the RootPanel
		if (RootPanel.get("nameFieldContainer") != null) {
			//main page
			RootPanel.get("nameFieldContainer").add(fileField);
			RootPanel.get("idFieldContainer").add(idField);
			RootPanel.get("sendButtonContainer").add(submitButton);
			RootPanel.get("responseContainer").add(responseField);
			RootPanel.get("monitorButtonContainer").add(monitorButton);
		} 
		else if (RootPanel.get("usernameFieldContainer") != null) {
			//login page
			RootPanel.get("usernameFieldContainer").add(usernameField);
			RootPanel.get("passwordFieldContainer").add(passwordField);
			RootPanel.get("loginButtonContainer").add(loginButton);
		}
		else {
			//results page
			RootPanel.get("homeButtonContainer").add(goHomeButton);			
			RootPanel.get("tableContainer").add(scrollPanel);
		    cellFormatter.setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
		    //cellFormatter.setColSpan(0, 0, 0);	
		    //we no longer use timer scheduling, just run it once
		    timer.run();			
			// Program a Timer to execute this every N seconds
		    //no more auto-refresh: user will refresh manually
		    //timer.scheduleRepeating(UPDATE_INTERVAL*1000);
		}
		// Create a handler for the loginButtonButton
		class LoginButtonHandler implements ClickHandler {
			public void onClick(ClickEvent event) {
				String username = usernameField.getText();
				String password = passwordField.getText();
				RootPanel.get().clear();
				Window.Location.assign("/sampleapp?username=" + username + "&password=" + password);
			}			
		}		
		loginButton.addClickHandler(new LoginButtonHandler());
		
		// Focus the cursor on the name field when the page loads
		fileField.setFocus(true);

		// Create a handler for the sendButton and nameField
		class SubmitHandler implements ClickHandler, KeyUpHandler {
			/**
			 * Fired when the user clicks on the sendButton.
			 */
			public void onClick(ClickEvent event) {
				doExec();
			}

			/**
			 * Fired when the user types in any field.
			 */
			public void onKeyUp(KeyUpEvent event) {
				if (event.getNativeKeyCode() == KeyCodes.KEY_ENTER) {
					doExec();
				}
			}

			/**
			 * Send the request to the server and wait for a response.
			 */
			private void doExec() {
				String imageId = idField.getText();
				if (imageId.length() == 0) {
					Window.alert(ERROR_ID_REQUIRED);
					return;
				}
				responseField.setHTML("<div style='border: 1px solid grey; color: grey;'>" + RUN_WAIT_MSG + "</div>");
				responseField.setVisible(true);

				// Send the input to the server.
				submitButton.setEnabled(false);
				sampleService.runAsync(fileField.getItemText(fileField.getSelectedIndex()), imageId,
						new AsyncCallback<String>() {
							public void onFailure(Throwable caught) {
								String result = "<div style='border: 1px solid red; color: red;'>" + SERVER_ERROR + caught.getMessage() + "</div>";
								responseField.setHTML(result);
								responseField.setVisible(true);
							}
							public void onSuccess(String commandString) {
								String result = "<div style='border: 1px solid green; color: green;'>" + "Command " + commandString + " started" + "</div>";
								responseField.setHTML(result);
								responseField.setVisible(true);
							}
						});
			}
		}
		// a handler for monitorButton
		class MonitorHandler implements ClickHandler {
			public void onClick(ClickEvent event) {
				RootPanel.get().clear();
				Window.Location.assign("/sampleapp/Jobs.jsp");
			}
		}
		// a handler for goHomeButton
		class GoHomeHandler implements ClickHandler {
			public void onClick(ClickEvent event) {
				RootPanel.get().clear();
				Window.Location.assign("/sampleapp/SampleApp.jsp");
			}
		}
		// Add button handlers
		submitButton.addClickHandler(new SubmitHandler());
		monitorButton.addClickHandler(new MonitorHandler());
		goHomeButton.addClickHandler(new GoHomeHandler());
	}

	private void getConfigParams() {
		sampleService.getConfigParams(new AsyncCallback<List<String>>() {
			public void onFailure(Throwable caught) {
				Window.alert(ERROR_GETTING_CONFIG + " " + caught.getMessage());
			}
			public void onSuccess(List<String> result) {
				populateFileField(result.get(0));
			}
		});
	}

	private void populateFileField(String currentDir) {
		if ( currentDir!=null && currentDir.startsWith("+")) {
			currentDir = currentDir.substring(1);
		}
		if (currentDir.indexOf(NO_ACCESS) >= 0) {
			Window.alert(NO_ACCESS_MSG);
			return;
		}
		sampleService.getFiles(currentDir, new AsyncCallback<List<String>>() {
			public void onFailure(Throwable caught) {
				Window.alert(ERROR_GETTING_FILES);
			}
			public void onSuccess(List<String> result) {
				fileField.clear();
				for (String file : result) {
					fileField.addItem(file);
				}
			}
		});
	}
	
	private class GetDataAsyncCallback<T> implements AsyncCallback<String[][]> {
		public void onFailure(Throwable caught) {
		}
		public void onSuccess(String[][] result) {
			if ( result == null) {
				timer.cancel();
			} 
			else {
				for (int i=0; i<result.length; i++) {
					addRow(flexTable, result[i]);
				}
			}
		}
	}
	
	private class ColNamesAsyncCallback<T> implements AsyncCallback<String[]> {
		public void onFailure(Throwable caught) {
		}
		public void onSuccess(String[] result) {
			addRow(flexTable, result);
		}
	}
	
	private void addRow(FlexTable flexTable, String []row) {
		int numRows = flexTable.getRowCount();
		for (int i=0; i<row.length; i++) {
			flexTable.setWidget(numRows, i, new HTML(row[i]));
		}
	}
}
