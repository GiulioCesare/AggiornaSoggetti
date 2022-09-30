/* 
 * Carica autori in indice tramite protocollo SBNMARC
 * 
 * Argentino Trombin
 * 15/02/2022
 * 
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URLEncoder;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PostMethod;

import it.finsiel.misc.DateUtil;

public class SendMessageIndiceXml {
    private String schemaVersionStr = "";
    private String poloStr = ""; // = "CFI";
    private String bibliotecaStr = ""; // = " CF";
    private String userIdStr = ""; // = "000001";
	private String livelloAutoritaAutoriPolo = ""; //  = livelloAutoritaDB;
	private String httpServer;
	private String servlet;
	private String loginUid; 
	private String loginPwd;
	private String iniziaElaborazioneDaRigaStr="";
	private int iniziaElaborazioneDaRiga;
	private String elaboraNRigheStr=""; // 0 = tutte
	private int elaboraNRighe; // 0 = tutte
	String logFileOut = "";
	private boolean logRequest=false; 
	private boolean logResponse=false; 

	
	//	BufferedWriter OutLog;
	FileWriter  OutLog;  

	
	int progress=0xff; 
    String msg;
	
    private HttpClient httpClient = null;
   
    private BufferedReader in;
    
	int requestElaborateCtr=0;
	int requestFalliteCtr=0;
	int requestOkCtr=0;
	
public void sendMsg(String requestXml)    
{
	String responseXml;
	requestElaborateCtr++;	
  	responseXml = eseguiRichiestaXml(requestXml);
  	if (responseXml.indexOf("<esito>0000</esito>") == -1)
  	{
	  		msg =	"\nERRORE - Request: "+requestXml+
	  				"\nERRORE - Response: "+responseXml;
			log(msg);
  		requestFalliteCtr++;
  	}
  	else
  	{
  		
		if (logRequest == true) // defaults to false
	  		log ("\nRequest: " + requestXml);
		if (logResponse == true)
	  		log ("\nResponse: " + responseXml);
  		requestOkCtr++;
	  	}
} // End sendMsg 









private String eseguiRichiestaXml(String requestXml)
{
    String response="";
    StringBuffer sbResponse = new StringBuffer();
	
    PostMethod post = new PostMethod(servlet); // "http://10.30.1.83/certificazione/servlet/serversbnmarc"
    post.addRequestHeader("Content-Type", "text/html; charset=UTF-8");
    
    
    post.setDoAuthentication( true );
    post.addParameter("testo_xml", requestXml);

    //    String requestXmlEncoded = URLEncoder.encode(requestXml);
    //System.out.println("Encoded Request "+requestXmlEncoded);
//    post.addParameter("testo_xml", requestXmlEncoded); // MUST ENCODE else server misinterprets special characters
    
    
    
    int status=0;;
    try {
        // execute the POST
        status = httpClient.executeMethod( post );
        if (status != 200)
            System.out.println("status: "+status);
        else
        {
          in = new BufferedReader(new InputStreamReader(post.getResponseBodyAsStream(), "UTF-8"));
          String line = null;
          while ((line = in.readLine()) != null)
          	sbResponse.append(line);
          in.close();
        }
        
	} catch (MalformedURLException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
    } finally {
        // release any connection resources used by the method
        post.releaseConnection();
//        System.out.println("status: "+status);
    }
    response = sbResponse.toString();
//    System.out.println( "response = "+response);
    return response;
    
}


private void openHttpConnection()
{
/*
    log.fatal(Object message);
    log.error(Object message);
    log.warn(Object message);
    log.info(Object message);
    log.debug(Object message);
    log.trace(Object message);
	
 */
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
    System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "false");
    System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire.header", "warn");
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.commons.httpclient", "warn");
       
    httpClient = new HttpClient();
    
    // pass our credentials to HttpClient, they will only be used for
    // authenticating to servers with realm "realm" on the host
    // "www.verisign.com", to authenticate against an arbitrary realm 
    // or host change the appropriate argument to null.
    httpClient.getState().setCredentials(
    		new AuthScope(httpServer, 80, AuthScope.ANY_REALM),
            new UsernamePasswordCredentials(loginUid, loginPwd)
            
            );
       
}


public static void main(String[] args) {
	
	if (args.length != 1)
	{
		System.out.println("SendMessageIndiceXml fileDiConfigurazione.cfg");
		System.exit(1);
	}

	String start = "\nSendMessageIndiceXml (c) ICCU 01/2022 - Autore: Argentino trombin";
//	String versione = "\nVer. 2022_02_18";
	String versione = "\nVer. 2022_02_28"; // gestione logRequest, logResponse

	
	System.out.print(start + versione);

	
	try {
		String fileMessaggiDaCaricare="";
		String stringAr[];

		SendMessageIndiceXml sendMessageIndiceXml = new SendMessageIndiceXml();
		
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
			
		for(String line; (line = br.readLine()) != null; ) {
			if (line.isEmpty() || line.charAt(0) == '#')
				continue;
			stringAr = line.trim().split("=");
			if (stringAr[0].equals("fileMessaggiDaCaricare"))
				fileMessaggiDaCaricare=stringAr[1];
			if (stringAr[0].equals("schemaVersionStr"))
				sendMessageIndiceXml.schemaVersionStr=stringAr[1];
			else if (stringAr[0].equals("poloStr"))
				sendMessageIndiceXml.poloStr=stringAr[1];
			else if (stringAr[0].equals("bibliotecaStr"))
				sendMessageIndiceXml.bibliotecaStr=stringAr[1];
			else if (stringAr[0].equals("userIdStr"))
				sendMessageIndiceXml.userIdStr=stringAr[1];
			else if (stringAr[0].equals("livelloAutoritaAutoriPolo"))
				sendMessageIndiceXml.livelloAutoritaAutoriPolo=stringAr[1];
			else if (stringAr[0].equals("httpServer"))
				sendMessageIndiceXml.httpServer=stringAr[1];
			else if (stringAr[0].equals("servlet"))
				sendMessageIndiceXml.servlet=stringAr[1];
			else if (stringAr[0].equals("loginUid"))
				sendMessageIndiceXml.loginUid=stringAr[1];
			else if (stringAr[0].equals("loginPwd"))
				sendMessageIndiceXml.loginPwd=stringAr[1];
			else if (stringAr[0].equals("iniziaElaborazioneDaRiga"))
			{
				sendMessageIndiceXml.iniziaElaborazioneDaRigaStr = stringAr[1];
				sendMessageIndiceXml.iniziaElaborazioneDaRiga = Integer.parseInt(stringAr[1]);
			}
			else if (stringAr[0].equals("elaboraNRighe"))
			{
				sendMessageIndiceXml.elaboraNRigheStr = stringAr[1];
				sendMessageIndiceXml.elaboraNRighe = Integer.parseInt(stringAr[1]);
			}
			else if (stringAr[0].startsWith("progress"))
				sendMessageIndiceXml.progress = Integer.parseInt(stringAr[1], 16);
			else if (stringAr[0].startsWith("logFileOut"))
				sendMessageIndiceXml.logFileOut = stringAr[1];

			else if (stringAr[0].startsWith("logRequest") && stringAr[1].startsWith("true"))
					sendMessageIndiceXml.logRequest = true; // defaults to false
			else if (stringAr[0].startsWith("logResponse") && stringAr[1].startsWith("true"))
					sendMessageIndiceXml.logResponse = true; // defaults to false
		
		}

		String parametriInput = 
				
				"\npoloStr = '"+sendMessageIndiceXml.poloStr+"'"
				+"\nbibliotecaStr = '"+sendMessageIndiceXml.bibliotecaStr+"'"
				+"\nuserIdStr = '"+sendMessageIndiceXml.userIdStr+"'"
				+"\nlivelloAutoritaAutoriPolo = '"+sendMessageIndiceXml.livelloAutoritaAutoriPolo+"'"
				+"\nhttpServer =  '"+sendMessageIndiceXml.httpServer+"'"
				+"\nservlet = '"+sendMessageIndiceXml.servlet+"'"
				+"\nloginUid = '"+sendMessageIndiceXml.loginUid+"'"
				+"\nloginPwd = '"+sendMessageIndiceXml.loginPwd+"'"
				+"\niniziaElaborazioneDaRiga = '"+sendMessageIndiceXml.iniziaElaborazioneDaRiga+"'"
				+"\nelaboraNRighe = '"+sendMessageIndiceXml.elaboraNRighe+"'"
				+"\n";

		
		// controlla che abbiamo tutti i parametri
		if (fileMessaggiDaCaricare.isEmpty()
			|| sendMessageIndiceXml.schemaVersionStr.isEmpty()
			|| sendMessageIndiceXml.poloStr.isEmpty()
			|| sendMessageIndiceXml.bibliotecaStr.isEmpty()
			|| sendMessageIndiceXml.userIdStr.isEmpty()
			|| sendMessageIndiceXml.servlet.isEmpty()
			|| sendMessageIndiceXml.loginUid.isEmpty()
			|| sendMessageIndiceXml.loginPwd.isEmpty()
			|| sendMessageIndiceXml.httpServer.isEmpty()
			|| sendMessageIndiceXml.iniziaElaborazioneDaRigaStr.isEmpty()
			|| sendMessageIndiceXml.elaboraNRigheStr.isEmpty()
			
			)
		{
			System.out.println("File da caricare vuoto o anca uno o piu parametri tra: " // , jdbcDriver, connectionUrl
					+parametriInput
					);
			System.exit(1);
		}

		
		
		// Apriamo il file di log
		try {
			System.out.println("File di log: "	+ sendMessageIndiceXml.logFileOut);
			sendMessageIndiceXml.OutLog = new FileWriter(sendMessageIndiceXml.logFileOut);				
			sendMessageIndiceXml.log(start);
			sendMessageIndiceXml.log("Parametri da file: "+parametriInput);
			
		} catch (Exception fnfEx) {
			fnfEx.printStackTrace();
			return;
		}
		
		sendMessageIndiceXml.openHttpConnection();
		
		br = new BufferedReader(new FileReader(fileMessaggiDaCaricare));
		int ctr = 0;
		for(String line; (line = br.readLine()) != null; ) {
			if (line.trim().length() <1)
				continue; // empy line
			if (line.charAt(0) == '#')
				continue; // commented line
			ctr++;
			
			if (ctr < sendMessageIndiceXml.iniziaElaborazioneDaRiga)
				continue;
				
			sendMessageIndiceXml.sendMsg(line);

if (sendMessageIndiceXml.elaboraNRighe != 0 && ctr >= (
		sendMessageIndiceXml.iniziaElaborazioneDaRiga+sendMessageIndiceXml.elaboraNRighe-1)
)
	break;
			

			if ((ctr & sendMessageIndiceXml.progress) == 0)
			{
				sendMessageIndiceXml.msg = "\nMessaggi elaborati:" +(ctr-sendMessageIndiceXml.iniziaElaborazioneDaRiga+1);
				System.out.print(sendMessageIndiceXml.msg);
				sendMessageIndiceXml.log(sendMessageIndiceXml.msg);
				
			}
			
			
		}
		sendMessageIndiceXml.msg = "\nInizio inserimento da riga:" +sendMessageIndiceXml.iniziaElaborazioneDaRiga + 
				"\nRequest elaborate:" +sendMessageIndiceXml.requestElaborateCtr + 
//				"\nAutori gia' esistenti:" +sendMessageIndiceXml.autoriGiaEsistentiCtr+
				"\nRequest andate bene:" +sendMessageIndiceXml.requestOkCtr+
				"\nRequest fallite:" +sendMessageIndiceXml.requestFalliteCtr+ 
				"\n" + DateUtil.getDate() + " " + DateUtil.getTime()
				;
		sendMessageIndiceXml.log(sendMessageIndiceXml.msg  );
		System.out.print(sendMessageIndiceXml.msg);

		sendMessageIndiceXml.OutLog.close();	// Chiudi log dopo aver caricato un file
		
	} catch (IOException e) {
		e.printStackTrace();
	}	
} // End main

void log(String s)
{
	try {
		OutLog.write(s);
		OutLog.flush();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

}
