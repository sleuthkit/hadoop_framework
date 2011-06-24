<!doctype html>
<html>
  <head>
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">

    <link type="text/css" rel="stylesheet" href="SampleApp.css">

    <title>TP Main Page</title>
    
    <script type="text/javascript" language="javascript" src="sampleapp/sampleapp.nocache.js"></script>
  </head>

  <body>

    <iframe src="javascript:''" id="__gwt_historyFrame" tabIndex='-1' style="position:absolute;width:0;height:0;border:0"></iframe>    
    <noscript>
      <div style="width: 22em; position: absolute; left: 50%; margin-left: -11em; color: red; background-color: white; border: 1px solid red; padding: 4px; font-family: sans-serif">
        Your web browser must have JavaScript enabled
        in order for this application to display correctly.
      </div>
    </noscript>

    <h1><%=org.sleuthkit.web.sampleapp.Constants.HOME_PAGE_TITLE%></h1>
    
    <table class="sampleapp_panel" align="center">
      <tr>
        <td colspan="2" style="font-weight:bold;"><%=org.sleuthkit.web.sampleapp.Constants.HOME_PAGE_INSTRUCTIONS%></td>
      </tr>
      <tr>
        <td><%=org.sleuthkit.web.sampleapp.Constants.HOME_PAGE_ID_LABEL%></td>
        <td id="idFieldContainer"></td>
      </tr>
      <tr>
        <td><%=org.sleuthkit.web.sampleapp.Constants.HOME_PAGE_FILE_LABEL%></td>
        <td id="nameFieldContainer"></td>
      </tr>
      <tr>
        <td colspan="2" id="sendButtonContainer"></td>
      </tr>
      <tr>
        <td colspan="2" id="responseContainer"></td>
      </tr>
      <tr>
        <td colspan="2" id="monitorButtonContainer"></td>
      </tr>
    </table>
  </body>
</html>
