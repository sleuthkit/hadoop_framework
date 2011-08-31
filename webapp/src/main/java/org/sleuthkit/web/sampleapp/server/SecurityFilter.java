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

import java.io.IOException;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.StringTokenizer;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 * This class is a servlet security filter, to make sure user is authorized to view a page in web application
 */
public class SecurityFilter implements javax.servlet.Filter {

	final static int SESSION_TIMEOUT = 60; 	// Session Time To Live in Minutes
	
	static ResourceBundle rb = null;
	static HashMap<String,String> credentials = null;
	
	@Override
	public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) 
		throws IOException, ServletException {
				
		boolean authorized = false;
		HttpSession session = ((HttpServletRequest)req).getSession();
		String authVal = (String)session.getAttribute("SAMPLE_GWT_AUTH");
		if (authVal != null)
			authorized = authVal.equals("y");

		if (authorized) {
			chain.doFilter(req, res);
		}
		else {
			// Check if login and password received
			String username = req.getParameter("username");
			String password = req.getParameter("password");
			if ( username!=null && password!=null) {
				if( checkCredentials(username, password)) {
					session.setAttribute("SAMPLE_GWT_AUTH", "y");
					session.setMaxInactiveInterval(SESSION_TIMEOUT*60);
					((HttpServletResponse)res).sendRedirect("/sampleapp/SampleApp.jsp");
				} 
				else {
					String loginUrl = "/sampleapp/login.jsp?failed=true";
					((HttpServletResponse)res).sendRedirect(loginUrl);
				}
			} 
			else {
				// Force the user to login
				String loginUrl = "/sampleapp/login.jsp";
				((HttpServletResponse)res).sendRedirect(loginUrl);
			}
		}
	}

	private boolean checkCredentials(String username, String password) {
		if( credentials == null) {
			readProps();
		}
		String storedPassword = credentials.get(username);
		if( storedPassword!= null && storedPassword.equals(password)) {
			return true;
		}
		return false;
	}
		
	static void readProps() {
		// Read the config parameters from properties file
		credentials = new HashMap<String,String>();
		try {
			rb = ResourceBundle.getBundle("sampleapp");
			String credentialsList = rb.getString("credentials");
			StringTokenizer st = new StringTokenizer(credentialsList, ",");
			String userpass = null;
			String username = null;
			String password = null;
			while (st.hasMoreElements()) {
				userpass = st.nextToken();
				username = userpass.substring(0, userpass.indexOf("@"));
				password = userpass.substring(userpass.indexOf("@")+1);
				credentials.put(username, password);
			}
			
		} 
		catch (Exception ex) {
			System.err.println("Error initializing application, cannot read configuration. Using default.");
			ex.printStackTrace(System.err);
			credentials.put("admin", "admin123");
		}
	}

	@Override
	public void destroy() {
	}

	@Override
	public void init(FilterConfig arg0) throws ServletException {
	}
}
