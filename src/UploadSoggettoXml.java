/* 
 * Aggiorna soggetti in indice tramite protocollo SBNMARC
 * 
 * Argentino Trombin
 * 02/01/2017
 * 
 * DEV:
 * 	/media/export54/indice/dp/cfi_soggetti/uploadSoggettiXml.cfg
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

public class UploadSoggettoXml {
    private String schemaVersionStr = "";// = "2.00"; // Di fatto trasversale su tutte le funzioni
    private String poloStr = ""; // = "CFI";
    private String bibliotecaStr = ""; // = " CF";
    private String userIdStr = ""; // = "000001";
	private String livelloAutoritaSoggettiPolo = ""; //  = livelloAutoritaDB;
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
    
	int soggettiElaboratiCtr=0;
	int soggettiInseritiCtr=0;
	int soggettiNonInseritiCtr=0;
	int soggettiGiaEsistentiCtr=0;
	

public void uploadSoggetto(String soggetto)    
{
    String requestXml;
	String responseXml;

	String [] soggettoAR = soggetto.split(" - ");
	
	String cid = soggettoAR[0];
	String dol_2 = soggettoAR[1];
	String dol_livello_aut_input = soggettoAR[2];
	String uteVarInput = soggettoAR[3];
	String dol_A = soggettoAR[4];
	
	StringBuffer sb_dol_X = new StringBuffer();
	for (int i=5; i < soggettoAR.length; i++)
		sb_dol_X.append("<x_250>"+soggettoAR[i]+"</x_250>");
	
	soggettiElaboratiCtr++;	
	
	//CREA xml per SOGGETTO da caricare 
	requestXml = 
			"<?xml version='1.0' encoding='UTF-8'?><SBNMarc schemaVersion='2.01'><SbnUser>"
			+ "<Biblioteca>"
			+ uteVarInput.substring(0,6)	//poloStr+bibliotecaStr
			+ "</Biblioteca>"
			+ "<UserId>"
			+ uteVarInput.substring(6)	//userIdStr
			+ "</UserId></SbnUser>"
//			+ "<SbnMessage><SbnRequest><Crea tipoControllo='Simile'><ElementoAut>"
			+ "<SbnMessage><SbnRequest><Crea tipoControllo='Conferma'><ElementoAut>" // 19/04/2017
			+ "<DatiElementoAut xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' tipoAuthority='SO' livelloAut='"
			+ dol_livello_aut_input // 05/06/2017 livelloAutoritaSoggettiPolo
			+"' xsi:type='SoggettoType'>"
			+ "<T001>"
			+ cid
			+ "</T001>"
			+ "<T250>"
			+ "<a_250>"+dol_A+"</a_250>"
			+ sb_dol_X.toString()
			+ "<c2_250>"+dol_2+"</c2_250>"
			+ "</T250>"
			+ "</DatiElementoAut></ElementoAut></Crea></SbnRequest></SbnMessage></SBNMarc>"
			+ "";
  	
//System.out.println("Request per cid: "+cid+" "+poloStr+bibliotecaStr+". "+requestXml);
  	
	  	responseXml = eseguiRichiestaXml(requestXml);
	  	if (responseXml.indexOf("<esito>0000</esito>") == -1)
	  	{
//	  		try {
		  		msg = "\nRequest per cid: "+cid+" "+poloStr+bibliotecaStr+". "+requestXml+
		  				"\nInserimento soggetto fallito per cid: "+cid+" "+poloStr+bibliotecaStr+". Response: "+responseXml;
		  		//System.out.print(msg);
				log(msg);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
	  		
	  		soggettiNonInseritiCtr++;
	  	}
	  	else
	  	{
	  		soggettiInseritiCtr++;
	  	}
}









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
//	System.out.println("Hello aggiorna soggetti - 05/12/2016");
	String start = "\nUploadSoggettoXml (c) ICCU 01/2017";
	
	if (args.length != 1)
	{
		System.out.println("UploadSoggettoXml fileDiConfigurazione");
		System.exit(1);
	}
	System.out.print(start);

	
	try {
		String fileSoggettiDaCaricare="";
		String stringAr[];

		UploadSoggettoXml uploadSoggettoXml = new UploadSoggettoXml();
		
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
			
		for(String line; (line = br.readLine()) != null; ) {
			if (line.isEmpty() || line.charAt(0) == '#')
				continue;
			stringAr = line.trim().split("=");
			if (stringAr[0].equals("fileSoggettiDaCaricare"))
				fileSoggettiDaCaricare=stringAr[1];
			if (stringAr[0].equals("schemaVersionStr"))
				uploadSoggettoXml.schemaVersionStr=stringAr[1];
			else if (stringAr[0].equals("poloStr"))
				uploadSoggettoXml.poloStr=stringAr[1];
			else if (stringAr[0].equals("bibliotecaStr"))
				uploadSoggettoXml.bibliotecaStr=stringAr[1];
			else if (stringAr[0].equals("userIdStr"))
				uploadSoggettoXml.userIdStr=stringAr[1];
			else if (stringAr[0].equals("livelloAutoritaSoggettiPolo"))
				uploadSoggettoXml.livelloAutoritaSoggettiPolo=stringAr[1];
			else if (stringAr[0].equals("httpServer"))
				uploadSoggettoXml.httpServer=stringAr[1];
			else if (stringAr[0].equals("servlet"))
				uploadSoggettoXml.servlet=stringAr[1];
			else if (stringAr[0].equals("loginUid"))
				uploadSoggettoXml.loginUid=stringAr[1];
			else if (stringAr[0].equals("loginPwd"))
				uploadSoggettoXml.loginPwd=stringAr[1];
			else if (stringAr[0].equals("iniziaElaborazioneDaRiga"))
			{
				uploadSoggettoXml.iniziaElaborazioneDaRigaStr = stringAr[1];
				uploadSoggettoXml.iniziaElaborazioneDaRiga = Integer.parseInt(stringAr[1]);
			}
			else if (stringAr[0].equals("elaboraNRighe"))
			{
				uploadSoggettoXml.elaboraNRigheStr = stringAr[1];
				uploadSoggettoXml.elaboraNRighe = Integer.parseInt(stringAr[1]);
			}
			else if (stringAr[0].startsWith("logFileOut"))
				uploadSoggettoXml.logFileOut = stringAr[1];

			else if (stringAr[0].startsWith("progress"))
				uploadSoggettoXml.progress = Integer.parseInt(stringAr[1], 16);
			
		}

		String parametriInput = 
				
				"\npoloStr = '"+uploadSoggettoXml.poloStr+"'"
				+"\nbibliotecaStr = '"+uploadSoggettoXml.bibliotecaStr+"'"
				+"\nuserIdStr = '"+uploadSoggettoXml.userIdStr+"'"
				+"\nlivelloAutoritaSoggettiPolo = '"+uploadSoggettoXml.livelloAutoritaSoggettiPolo+"'"
				+"\nhttpServer =  '"+uploadSoggettoXml.httpServer+"'"
				+"\nservlet = '"+uploadSoggettoXml.servlet+"'"
				+"\nloginUid = '"+uploadSoggettoXml.loginUid+"'"
				+"\nloginPwd = '"+uploadSoggettoXml.loginPwd+"'"
				+"\niniziaElaborazioneDaRiga = '"+uploadSoggettoXml.iniziaElaborazioneDaRiga+"'"
				+"\nelaboraNRighe = '"+uploadSoggettoXml.elaboraNRighe+"'"
				+"\n";

		
		// controlla che abbiamo tutti i parametri
		if (fileSoggettiDaCaricare.isEmpty()
			|| uploadSoggettoXml.schemaVersionStr.isEmpty()
			|| uploadSoggettoXml.poloStr.isEmpty()
			|| uploadSoggettoXml.bibliotecaStr.isEmpty()
			|| uploadSoggettoXml.userIdStr.isEmpty()
			|| uploadSoggettoXml.livelloAutoritaSoggettiPolo.isEmpty()
			|| uploadSoggettoXml.servlet.isEmpty()
			|| uploadSoggettoXml.loginUid.isEmpty()
			|| uploadSoggettoXml.loginPwd.isEmpty()
			|| uploadSoggettoXml.httpServer.isEmpty()
			|| uploadSoggettoXml.iniziaElaborazioneDaRigaStr.isEmpty()
			|| uploadSoggettoXml.elaboraNRigheStr.isEmpty()
			
			)
		{
			System.out.println("Manca uno o piu parametri tra: " // , jdbcDriver, connectionUrl
					+parametriInput
					);
			System.exit(1);
		}

		
		
		// Apriamo il file di log
		try {
			System.out.println("File di log: "	+ uploadSoggettoXml.logFileOut);
//			uploadSoggettoXml.OutLog = new BufferedWriter(new FileWriter(uploadSoggettoXml.logFileOut));				
			uploadSoggettoXml.OutLog = new FileWriter(uploadSoggettoXml.logFileOut);				
			uploadSoggettoXml.log(start);
			uploadSoggettoXml.log("Parametri da file: "+parametriInput);
			
		} catch (Exception fnfEx) {
			fnfEx.printStackTrace();
			return;
		}
		
		uploadSoggettoXml.openHttpConnection();
		
		br = new BufferedReader(new FileReader(fileSoggettiDaCaricare));
		int ctr = 0;
		for(String line; (line = br.readLine()) != null; ) {
			if (line.trim().length() > 0 && line.charAt(0) == '#')
				continue;
			ctr++;
			
			if (ctr < uploadSoggettoXml.iniziaElaborazioneDaRiga)
				continue;
				
			uploadSoggettoXml.uploadSoggetto(line);

if (uploadSoggettoXml.elaboraNRighe != 0 && ctr >= (
		uploadSoggettoXml.iniziaElaborazioneDaRiga+uploadSoggettoXml.elaboraNRighe-1)
)
	break;
			

			if ((ctr & uploadSoggettoXml.progress) == 0)
			{
				uploadSoggettoXml.msg = "\nSoggetti elaborati:" +(ctr-uploadSoggettoXml.iniziaElaborazioneDaRiga+1);
				System.out.print(uploadSoggettoXml.msg);
				uploadSoggettoXml.log(uploadSoggettoXml.msg);
				
			}
			
			
		}
		uploadSoggettoXml.msg = "\nInizio inserimento da riga:" +uploadSoggettoXml.iniziaElaborazioneDaRiga + 
				"\nSoggetti elaborati:" +uploadSoggettoXml.soggettiElaboratiCtr + 
				"\nSoggetti gia' esistenti:" +uploadSoggettoXml.soggettiGiaEsistentiCtr+
				"\nSoggetti inseriti:" +uploadSoggettoXml.soggettiInseritiCtr+
				"\nSoggetti non inseriti:" +uploadSoggettoXml.soggettiNonInseritiCtr+ 
				"\n" + DateUtil.getDate() + " " + DateUtil.getTime()
				;
		uploadSoggettoXml.log(uploadSoggettoXml.msg  );
		System.out.print(uploadSoggettoXml.msg);

		uploadSoggettoXml.OutLog.close();	// Chiudi log dopo aver caricato un file
		
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
