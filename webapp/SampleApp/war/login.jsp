<!doctype html>
<html>
  <head>
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
    <link type="text/css" rel="stylesheet" href="SampleApp.css">
    <title>Sample GWT Application - Login</title>
    
    <script type="text/javascript" language="javascript" src="sampleapp/sampleapp.nocache.js"></script>
  </head>

  	<%
	if(request.getParameter("failed") != null) {
	%>
	<body onload="alert('Invalid username or password');">
	<%
	} else {
	%>
	<body>
	<%
	}
	%>
  
    <noscript>
      <div style="width: 22em; position: absolute; left: 50%; margin-left: -11em; color: red; background-color: white; border: 1px solid red; padding: 4px; font-family: sans-serif">
        Your web browser must have JavaScript enabled
        in order for this application to display correctly.
      </div>
    </noscript>

    <h1><%=org.sleuthkit.web.sampleapp.Constants.LOGIN_PAGE_TITLE%></h1>
    
    <table align="center">
      <tr>
        <td><%=org.sleuthkit.web.sampleapp.Constants.LOGIN_USERNAME_LABEL%></td>
        <td id="usernameFieldContainer"></td>
      </tr>
      <tr>
        <td><%=org.sleuthkit.web.sampleapp.Constants.LOGIN_PASSWORD_LABEL%> </td>
        <td id="passwordFieldContainer"></td>
      </tr>
      <tr>
        <td colspan="2" id="loginButtonContainer"></td>
      </tr>
    </table>
  </body>
</html>
