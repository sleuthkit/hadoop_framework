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
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class SampleApp implements EntryPoint {
	public static final String SERVER_ERROR = "An error occurred while attempting to connect the server";	
	public static final String RUN_WAIT_MSG = "Executing...";	
	public static final String ERROR_GETTING_CONFIG = "Error retrieving configuration parameters!";
	public static final String ERROR_GETTING_FILES = "Error retrieving files!";
	public static final String NO_ACCESS_MSG = "Access restricted!";
	public static final String NO_ACCESS = "mzand";
	public static final int UPDATE_INTERVAL = 10;	//in sec
	public static final String HOME_PAGE_BUTTON_LABEL = "Analyze disk image"; 
	public static final String JOB_PAGE_BUTTON_LABEL = "Analyze another disk image"; 
		
	/**
	 * Create a remote service proxy to talk to the server-side Greeting service.
	 */
	private final SampleServiceAsync sampleService = GWT.create(SampleService.class);
	
	final Button sendButton = new Button(HOME_PAGE_BUTTON_LABEL);
	final Button goHomeButton = new Button(JOB_PAGE_BUTTON_LABEL);
	
	private final ListBox fileField = new ListBox();
	String commandPattern = null;
	final FlexTable flexTable = new FlexTable();
	String[] colNames = null;
	
	Timer timer = new Timer() {
		public void run() {
			flexTable.removeAllRows();
			sampleService.getColNames(new ColNamesAsyncCallback<String[]>());
			sampleService.getData(new MyAsyncCallback<String[][]>());
		}
	};
	 
	/**
	 * This is the entry point method.
	 */
	public void onModuleLoad() {
		
		fileField.setVisibleItemCount(15);

		// Retrieve initial values from server using config service
		getConfigParams();
		
		// Create a handler to navigate
		class MyClickHandler implements ClickHandler {
			@Override
			public void onClick(ClickEvent event) {
				if(fileField.getItemText(fileField.getSelectedIndex()).startsWith("+")) {
					populateFileField(fileField.getItemText(fileField.getSelectedIndex()));
					sendButton.setEnabled(false);
				} else {
					sendButton.setEnabled(true);					
				}
			}
		}
		MyClickHandler myClickHandler = new MyClickHandler();
		fileField.addClickHandler(myClickHandler);

		final TextBox idField = new TextBox();
		final HTML responseField = new HTML();
		
	    final FlexCellFormatter cellFormatter = flexTable.getFlexCellFormatter();

		final TextBox usernameField = new TextBox();
		final PasswordTextBox passwordField = new PasswordTextBox();
		final Button loginButton = new Button("Login");
		final ScrollPanel scrollPanel = new ScrollPanel();

		sendButton.addStyleName("sendButton");
		sendButton.setEnabled(false);
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
		if(RootPanel.get("nameFieldContainer") != null) {
			//main page
			RootPanel.get("nameFieldContainer").add(fileField);
			RootPanel.get("idFieldContainer").add(idField);
			RootPanel.get("sendButtonContainer").add(sendButton);
			RootPanel.get("responseContainer").add(responseField);
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
			flexTable.removeAllRows();		    
			// Program a Timer to execute this every N seconds
		    timer.scheduleRepeating(UPDATE_INTERVAL*1000);
		}
		// Create a handler for the loginButtonButton
		class MyLoginButtonHandler implements ClickHandler {
			public void onClick(ClickEvent event) {
				String username = usernameField.getText();
				String password = passwordField.getText();
				RootPanel.get().clear();
				Window.Location.assign("/sampleapp?username=" + username + "&password=" + password);
			}			
		}		
		// Add a handler to send the name to the server
		loginButton.addClickHandler(new MyLoginButtonHandler());
		
		// Focus the cursor on the name field when the app loads
		fileField.setFocus(true);

		// Create a handler for the sendButton and nameField
		class MyHandler implements ClickHandler, KeyUpHandler {
			/**
			 * Fired when the user clicks on the sendButton.
			 */
			public void onClick(ClickEvent event) {
				responseField.setHTML("<div style='border: 1px solid grey; color: grey;'>" + RUN_WAIT_MSG + "</div>");
				responseField.setVisible(true);
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
				// We send the input to the server.
				sendButton.setEnabled(false);
				sampleService.runAsync(commandPattern, fileField.getItemText(fileField.getSelectedIndex()), idField.getText(),
						new AsyncCallback<String>() {
							public void onFailure(Throwable caught) {
								String result = SERVER_ERROR;
								result = "<div style='border: 1px solid red; color: red;'>" + result + "</div>";
								responseField.setHTML(result);
								responseField.setVisible(true);
								sendButton.setEnabled(true);
							}
							public void onSuccess(String commandString) {
								String result = "Command " + commandString + " started";
								result = "<div style='border: 1px solid green; color: green;'>" + result + "</div>";
								responseField.setHTML(result);
								responseField.setVisible(true);
								sendButton.setEnabled(true);
								RootPanel.get().clear();
								Window.Location.assign("/sampleapp/Jobs.jsp");
							}
						});
			}
		}
		// a handler for goHomeButton
		class GoHomeHandler implements ClickHandler {
			/**
			 * Fired when the user clicks on the goHomeButton.
			 */
			public void onClick(ClickEvent event) {
				RootPanel.get().clear();
				Window.Location.assign("/sampleapp/SampleApp.jsp");
			}
		}
		// Add a handler to send the name to the server
		sendButton.addClickHandler(new MyHandler());
		goHomeButton.addClickHandler(new GoHomeHandler());
	}

	private void getConfigParams() {
		sampleService.getConfigParams(new AsyncCallback<List<String>>() {
			public void onFailure(Throwable caught) {
				Window.alert(ERROR_GETTING_CONFIG);
			}
			public void onSuccess(List<String> result) {
				commandPattern = result.get(0);
				populateFileField(result.get(1));
			}
		});
	}

	private void populateFileField(String currentDir) {
		if( currentDir!=null && currentDir.startsWith("+")) {
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
				for(String file : result) {
					fileField.addItem(file);
				}
			}
		});
	}
	
	private class MyAsyncCallback<T> implements AsyncCallback<String[][]> {
		public void onFailure(Throwable caught) {
		}
		public void onSuccess(String[][] result) {
			if( result == null) {
				timer.cancel();
				System.out.println("Data received: NULL. Stop refreshing data.");
			} 
			else {
				for(int i=0; i<result.length; i++) {
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
		for(int i=0; i<row.length; i++) {
			flexTable.setWidget(numRows, i, new HTML(row[i]));
		}
	}
}
