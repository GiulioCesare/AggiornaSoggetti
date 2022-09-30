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

public class UploadAutoriXml {
    private String schemaVersionStr = "";// = "2.00"; // Di fatto trasversale su tutte le funzioni
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
//	BufferedWriter OutLog;
	FileWriter  OutLog;  

	
	int progress=0xff; 
    String msg;
	
    private HttpClient httpClient = null;
   
    private BufferedReader in;
    
	int autoriElaboratiCtr=0;
	int autoriInseritiCtr=0;
	int autoriNonInseritiCtr=0;
	int autoriGiaEsistentiCtr=0;
	

public void uploadAutore(String riga_autore)    
{
    String requestXml;
	String responseXml;

	String [] autoreAR = riga_autore.split("\\|", Integer.MAX_VALUE); // Manages empy fields
	
	String vidNonSbn = autoreAR[0];
	String tipoNomeAutore = autoreAR[1];
	String dolA = "<a_200>"+autoreAR[2]+"</a_200>";
	String dolB = autoreAR[3].length() > 0 ? "<b_200>" +autoreAR[3] +"</b_200>" : "";
	String qualificazioneGenerica = autoreAR[4].length() > 0 ? "<c_200>" +autoreAR[4] +"</c_200>" : "";
	String qualificazioneData = autoreAR[5].length() > 0 ? "<f_200>" +autoreAR[5] +"</f_200>" : "";
	

	autoriElaboratiCtr++;	

	// CREA AUTORE
	requestXml = 
		"<?xml version='1.0' encoding='UTF-8'?>"
		+"<SBNMarc schemaVersion='2.03'>"
		+"<SbnUser><Biblioteca>"
		+ poloStr+bibliotecaStr	// "XXXAMM"
		+ "</Biblioteca>"
		+"<UserId>"
		+ userIdStr //"000001"
		+ "</UserId></SbnUser>"
		+"<SbnMessage>"
		+"<SbnRequest>"
		+"<Crea tipoControllo='Simile'>"
		+"<ElementoAut>"
		+"<DatiElementoAut tipoAuthority='AU' livelloAut='05' formaNome='A' tipoNome='"
		+tipoNomeAutore 
		+"' xsi:type='AutorePersonaleType' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>"
		+"<T001>0000000000</T001>"
		+"<T152><a_152>REICAT</a_152></T152>"
		+"<T200 id2='0'>"
		+dolA
		+dolB
		+qualificazioneGenerica
		+qualificazioneData
		+"</T200>"
		+"<T801><a_801>IT</a_801><b_801>ICCU</b_801></T801>"
		+"</DatiElementoAut>"
		+"</ElementoAut>"
		+"</Crea>"
		+"</SbnRequest>"
		+"</SbnMessage>"
		+"</SBNMarc>"
		+ "";
	
	
//System.out.println("Request per vidNonSbn: "+vidNonSbn+" "+poloStr+bibliotecaStr+". "+requestXml);
//if (true)
//	return;

	  	responseXml = eseguiRichiestaXml(requestXml);
	  	if (responseXml.indexOf("<esito>0000</esito>") == -1)
	  	{
		  		msg = "\nRequest per vidNonSbn: "+vidNonSbn+" "+poloStr+bibliotecaStr+". "+requestXml+
		  				"\nInserimento autore fallito per vidNonSbn: "+vidNonSbn+" "+poloStr+bibliotecaStr+". Response: "+responseXml;
				log(msg);
	  		autoriNonInseritiCtr++;
	  	}
	  	else
	  	{
	  		msg = "\nInserito autore per vidNonSbn: "+vidNonSbn+ " Response: " + responseXml;
			log(msg);

	  		
	  		autoriInseritiCtr++;
	  	}
	} // End uploadAutore 









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
	String start = "\nUploadAutoriXml (c) ICCU 01/2022 - Autore: Argentino trombin 2022_02_18";
	
	if (args.length != 1)
	{
		System.out.println("UploadAutoriXml fileDiConfigurazione");
		System.exit(1);
	}
	System.out.print(start);

	
	try {
		String fileAutoriDaCaricare="";
		String stringAr[];

		UploadAutoriXml uploadAutoriXml = new UploadAutoriXml();
		
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
			
		for(String line; (line = br.readLine()) != null; ) {
			if (line.isEmpty() || line.charAt(0) == '#')
				continue;
			stringAr = line.trim().split("=");
			if (stringAr[0].equals("fileAutoriDaCaricare"))
				fileAutoriDaCaricare=stringAr[1];
			if (stringAr[0].equals("schemaVersionStr"))
				uploadAutoriXml.schemaVersionStr=stringAr[1];
			else if (stringAr[0].equals("poloStr"))
				uploadAutoriXml.poloStr=stringAr[1];
			else if (stringAr[0].equals("bibliotecaStr"))
				uploadAutoriXml.bibliotecaStr=stringAr[1];
			else if (stringAr[0].equals("userIdStr"))
				uploadAutoriXml.userIdStr=stringAr[1];
			else if (stringAr[0].equals("livelloAutoritaAutoriPolo"))
				uploadAutoriXml.livelloAutoritaAutoriPolo=stringAr[1];
			else if (stringAr[0].equals("httpServer"))
				uploadAutoriXml.httpServer=stringAr[1];
			else if (stringAr[0].equals("servlet"))
				uploadAutoriXml.servlet=stringAr[1];
			else if (stringAr[0].equals("loginUid"))
				uploadAutoriXml.loginUid=stringAr[1];
			else if (stringAr[0].equals("loginPwd"))
				uploadAutoriXml.loginPwd=stringAr[1];
			else if (stringAr[0].equals("iniziaElaborazioneDaRiga"))
			{
				uploadAutoriXml.iniziaElaborazioneDaRigaStr = stringAr[1];
				uploadAutoriXml.iniziaElaborazioneDaRiga = Integer.parseInt(stringAr[1]);
			}
			else if (stringAr[0].equals("elaboraNRighe"))
			{
				uploadAutoriXml.elaboraNRigheStr = stringAr[1];
				uploadAutoriXml.elaboraNRighe = Integer.parseInt(stringAr[1]);
			}
			else if (stringAr[0].startsWith("logFileOut"))
				uploadAutoriXml.logFileOut = stringAr[1];

			else if (stringAr[0].startsWith("progress"))
				uploadAutoriXml.progress = Integer.parseInt(stringAr[1], 16);
			
		}

		String parametriInput = 
				
				"\npoloStr = '"+uploadAutoriXml.poloStr+"'"
				+"\nbibliotecaStr = '"+uploadAutoriXml.bibliotecaStr+"'"
				+"\nuserIdStr = '"+uploadAutoriXml.userIdStr+"'"
				+"\nlivelloAutoritaAutoriPolo = '"+uploadAutoriXml.livelloAutoritaAutoriPolo+"'"
				+"\nhttpServer =  '"+uploadAutoriXml.httpServer+"'"
				+"\nservlet = '"+uploadAutoriXml.servlet+"'"
				+"\nloginUid = '"+uploadAutoriXml.loginUid+"'"
				+"\nloginPwd = '"+uploadAutoriXml.loginPwd+"'"
				+"\niniziaElaborazioneDaRiga = '"+uploadAutoriXml.iniziaElaborazioneDaRiga+"'"
				+"\nelaboraNRighe = '"+uploadAutoriXml.elaboraNRighe+"'"
				+"\n";

		
		// controlla che abbiamo tutti i parametri
		if (fileAutoriDaCaricare.isEmpty()
			|| uploadAutoriXml.schemaVersionStr.isEmpty()
			|| uploadAutoriXml.poloStr.isEmpty()
			|| uploadAutoriXml.bibliotecaStr.isEmpty()
			|| uploadAutoriXml.userIdStr.isEmpty()
//			|| uploadAutoriXml.livelloAutoritaAutoriPolo.isEmpty()
			|| uploadAutoriXml.servlet.isEmpty()
			|| uploadAutoriXml.loginUid.isEmpty()
			|| uploadAutoriXml.loginPwd.isEmpty()
			|| uploadAutoriXml.httpServer.isEmpty()
			|| uploadAutoriXml.iniziaElaborazioneDaRigaStr.isEmpty()
			|| uploadAutoriXml.elaboraNRigheStr.isEmpty()
			
			)
		{
			System.out.println("Manca uno o piu parametri tra: " // , jdbcDriver, connectionUrl
					+parametriInput
					);
			System.exit(1);
		}

		
		
		// Apriamo il file di log
		try {
			System.out.println("File di log: "	+ uploadAutoriXml.logFileOut);
			uploadAutoriXml.OutLog = new FileWriter(uploadAutoriXml.logFileOut);				
			uploadAutoriXml.log(start);
			uploadAutoriXml.log("Parametri da file: "+parametriInput);
			
		} catch (Exception fnfEx) {
			fnfEx.printStackTrace();
			return;
		}
		
		uploadAutoriXml.openHttpConnection();
		
		br = new BufferedReader(new FileReader(fileAutoriDaCaricare));
		int ctr = 0;
		for(String line; (line = br.readLine()) != null; ) {
			if (line.trim().length() <1)
				continue; // empy line
			if (line.charAt(0) == '#')
				continue; // commented line
			ctr++;
			
			if (ctr < uploadAutoriXml.iniziaElaborazioneDaRiga)
				continue;
				
			uploadAutoriXml.uploadAutore(line);

if (uploadAutoriXml.elaboraNRighe != 0 && ctr >= (
		uploadAutoriXml.iniziaElaborazioneDaRiga+uploadAutoriXml.elaboraNRighe-1)
)
	break;
			

			if ((ctr & uploadAutoriXml.progress) == 0)
			{
				uploadAutoriXml.msg = "\nAutori elaborati:" +(ctr-uploadAutoriXml.iniziaElaborazioneDaRiga+1);
				System.out.print(uploadAutoriXml.msg);
				uploadAutoriXml.log(uploadAutoriXml.msg);
				
			}
			
			
		}
		uploadAutoriXml.msg = "\nInizio inserimento da riga:" +uploadAutoriXml.iniziaElaborazioneDaRiga + 
				"\nAutori elaborati:" +uploadAutoriXml.autoriElaboratiCtr + 
				"\nAutori gia' esistenti:" +uploadAutoriXml.autoriGiaEsistentiCtr+
				"\nAutori inseriti:" +uploadAutoriXml.autoriInseritiCtr+
				"\nAutori non inseriti:" +uploadAutoriXml.autoriNonInseritiCtr+ 
				"\n" + DateUtil.getDate() + " " + DateUtil.getTime()
				;
		uploadAutoriXml.log(uploadAutoriXml.msg  );
		System.out.print(uploadAutoriXml.msg);

		uploadAutoriXml.OutLog.close();	// Chiudi log dopo aver caricato un file
		
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
