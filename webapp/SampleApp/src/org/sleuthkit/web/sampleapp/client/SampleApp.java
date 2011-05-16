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

	/****************************************************/
	/* LITERALS 										*/
	/****************************************************/
	public static final String SERVER_ERROR = "An error occurred while attempting to connect the server";
	
	public static final String RUN_WAIT_MSG = "Executing...";	
	public static final String ERROR_GETTING_CONFIG = "Error retrieving configuration parameters!";
	public static final String ERROR_GETTING_FILES = "Error retrieving files!";
	public static final String NO_ACCESS_MSG = "Access restricted!";
	public static final String NO_ACCESS = "mzand";

	public static final String TABLE_HEADER = "Results table";
	public static final int UPDATE_INTERVAL = 10;	//in sec
		
	/**
	 * Create a remote service proxy to talk to the server-side Greeting service.
	 */
	private final SampleServiceAsync sampleService = GWT.create(SampleService.class);
	
	final Button sendButton = new Button("Execute Command");
	private final ListBox fileField = new ListBox();
	final TextBox cmdPatField = new TextBox();
	final FlexTable flexTable = new FlexTable();
	String[] colNames = null;
	
	Timer timer = new Timer() {
		public void run() {
			sampleService.getData(new MyAsyncCallback<String[][]>());
		}
	};
	 
	/**
	 * This is the entry point method.
	 */
	public void onModuleLoad() {
		
		fileField.setVisibleItemCount(10);

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
		final ScrollPanel sp = new ScrollPanel();

		sendButton.addStyleName("sendButton");
		sendButton.setEnabled(false);
		cmdPatField.setWidth("400px");
		idField.setWidth("400px");
		responseField.setWidth("640px");
		responseField.setVisible(false);

		sp.add(flexTable);
		sp.setSize("800px", "200px");
		sp.addStyleName("cw-ScollPanel");
		
	    flexTable.addStyleName("cw-FlexTable");
	    flexTable.setCellSpacing(2);
	    flexTable.setCellPadding(3);
		
		// Add the fields to the RootPanel
		if( RootPanel.get("nameFieldContainer") != null) {
			RootPanel.get("cmdPatFieldContainer").add(cmdPatField);
			RootPanel.get("nameFieldContainer").add(fileField);
			RootPanel.get("idFieldContainer").add(idField);
			RootPanel.get("sendButtonContainer").add(sendButton);
			RootPanel.get("responseContainer").add(responseField);
		} else {
			RootPanel.get("usernameFieldContainer").add(usernameField);
			RootPanel.get("passwordFieldContainer").add(passwordField);
			RootPanel.get("loginButtonContainer").add(loginButton);
		}

		// Create a handler for the loginButtonButton
		class MyLoginButtonHandler implements ClickHandler {
			public void onClick(ClickEvent event) {
				String username = usernameField.getText();
				String password = passwordField.getText();
				Window.Location.assign("/?username=" + username + "&password=" + password);
			}			
		}		
		// Add a handler to send the name to the server
		MyLoginButtonHandler mlbhandler = new MyLoginButtonHandler();
		loginButton.addClickHandler(mlbhandler);
		
		// Focus the cursor on the name field when the app loads
		fileField.setFocus(true);

		// Create a handler for the sendButton and nameField
		class MyHandler implements ClickHandler, KeyUpHandler {
			/**
			 * Fired when the user clicks on the sendButton.
			 */
			public void onClick(ClickEvent event) {
				responseField.setHTML("<div style='border: 1px solid grey; color: grey;'>" +
						RUN_WAIT_MSG +
						"</div>");
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
				
				if (cmdPatField.getText().indexOf(NO_ACCESS) >= 0) {
					Window.alert(NO_ACCESS_MSG);
					return;
				}
				// We send the input to the server.
				sendButton.setEnabled(false);
				sampleService.runAsync(cmdPatField.getText(),
								   fileField.getItemText(fileField.getSelectedIndex()),
								   idField.getText(),
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
								
								RootPanel.get("tableContainer").add(sp);
								flexTable.removeAllRows();
							    cellFormatter.setHorizontalAlignment(
							            0, 0, HasHorizontalAlignment.ALIGN_CENTER);
							    flexTable.setHTML(0, 0, TABLE_HEADER);
							    cellFormatter.setColSpan(0, 0, 3);
							    
								sampleService.getColNames(new ColNamesAsyncCallback<String[]>());

								// Program a Timer to execute this every N seconds
							    timer.scheduleRepeating(UPDATE_INTERVAL*1000);
							}
						});
			}
		}

		// Add a handler to send the name to the server
		MyHandler handler = new MyHandler();
		sendButton.addClickHandler(handler);
	}

	private void getConfigParams() {
		sampleService.getConfigParams(new AsyncCallback<List<String>>() {
			public void onFailure(Throwable caught) {
				Window.alert(ERROR_GETTING_CONFIG);
			}
			public void onSuccess(List<String> result) {
				cmdPatField.setText(result.get(0));
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
			} else {
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
