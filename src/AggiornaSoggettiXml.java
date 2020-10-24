/* 
 * Aggiorna soggetti in indice tramite protocollo SBNMARC
 * 
 * Argentino Trombin
 * 07/12/2016
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PostMethod;

public class AggiornaSoggettiXml {
    private String schemaVersionStr;// = "2.00"; // Di fatto trasversale su tutte le funzioni
    private String poloStr; // = "CFI";
    private String bibliotecaStr; // = " CF";
    private String userIdStr; // = "import";
	private String livelloAutoritaPolo; //  = livelloAutoritaDB;
	private int livelloAutoritaPolo_int;
	
//    private URL url; // = new URL("http://192.168.20.38:9080/indice/servlet/serversbnmarc");
//    private URLConnection conn; // conn = url.openConnection();
    private BufferedWriter out; // = new BufferedWriter( new OutputStreamWriter( conn.getOutputStream() ) );
    private BufferedReader in;
	private String httpServer;
	private String servlet;
	private String loginUid; 
	private String loginPwd;
	private String iniziaElaborazioneDaRigaStr="";
	private int iniziaElaborazioneDaRiga;
	private String elaboraNRigheStr=""; // 0 = tutte
	private int elaboraNRighe; // 0 = tutte
	
	
	private HttpClient httpClient = null;
    
	String logFileOut = "";
	
	// BufferedWriter OutLog;
	FileWriter  OutLog;  

	
	int progress=0xff; 
    String msg;

    String cidStr;
    String edizione;
    String livello_aut_sogg_in_polo;
    String ute_var_sogg_polo, ute_var_sogg_indice;
	String descrittori[];
    
	int soggettiElaboratiCtr=0;
	int soggettiInseritiCtr=0;

	

public void elaboraRispostaRicercaSoggetto(String response)    
{
    String timeStamp="";
    String livelloAutoritaDB;
    String tsIns="";


	
	if (response.indexOf("<esito>0000</esito>") == -1)
	{
			msg = "\nRicerca fallita per CID: "+cidStr+". Risposta: "+response;
			log(msg);
	}
    else
    {
    	// Troviamo il livello di authority del soggetto (primo della lista) per il confronto
    	int laStart = response.indexOf("livelloAut=\"");
    	if (laStart == -1)
    	{
        		msg = "\nNon trovo il livello di authority";
				log(msg);
    	}
    	
    	int laEnd = response.indexOf("\"", laStart+"livelloAut=\"".length());
    	
		livelloAutoritaDB = response.substring(laStart+"livelloAut=\"".length(), laEnd); 
//        System.out.println("livello di authority = " + livelloAutoritaDB);
    	
    	
    	// Troviamo il timestamp del soggetto (primo T005) per poter fare l'aggiornamento
    	int tsStart = response.indexOf("<T005>");
    	int tsEnd = response.indexOf("</T005>");
    	if (tsStart == -1 || tsEnd == -1)
    	{
        		msg = "\nNon trovo il timestamp";
				log(msg);
    	}
    	else
    	{
    		timeStamp = response.substring(tsStart+"<T005>".length(), tsEnd); 
//            System.out.println("timestamp = " + timeStamp);
    	}
    	
    	// Troviamo la data di inserimento
    	int tsInsStart = response.indexOf("<a_100_0>");
    	int tsInsEnd = response.indexOf("</a_100_0>");
    	if (tsInsStart == -1 || tsInsEnd == -1)
    	{
        		msg = "\nNon trovo la data di inserimento";
				log(msg);
    	}
    	else
    	{
    		tsIns = response.substring(tsInsStart+"<a_100_0>".length(), tsInsEnd); 
//            System.out.println("tsIns = " + tsIns);
    	}
    	
    	
    	// Costruiamo il messaggio per l'aggiornamento del soggetto
    	StringBuffer sbAltriDescrittori = new StringBuffer();
    	for (int i=1; i < descrittori.length; i++)
    		sbAltriDescrittori.append("<x_250>"+descrittori[i]+"</x_250>");
    	
    	
    	
    	
    	String requestXml_p1 = "<?xml version='1.0' encoding='UTF-8'?>"
    			+ "<SBNMarc schemaVersion='"+schemaVersionStr+"'><SbnUser>"
    			+ "<Biblioteca>";
//    			+poloStr+bibliotecaStr
		String requestXml_p2 ="</Biblioteca><UserId>";
//    			+ ute_var_sogg_polo	//userIdStr
  		String requestXml_p3 ="</UserId></SbnUser>"
    			+ "<SbnMessage><SbnRequest><Modifica tipoControllo='Conferma'><ElementoAut>"
    			+ "<DatiElementoAut tipoAuthority='SO' livelloAut='";
  				//+ livello_aut_sogg_in_polo	// livelloAutoritaPolo 
  		String requestXml_p4 ="' statoRecord='c' xsi:type='SoggettoType' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>"
    			+ "<T001>"+cidStr+"</T001>"
    			+ "<T005>"+timeStamp+"</T005>"
    			+ "<T100><a_100_0>"+tsIns+"</a_100_0></T100>"
    			+ "<T250>"
    			+ "<a_250>"+descrittori[0]+"</a_250>"
    			+ sbAltriDescrittori.toString()  //"<x_250>"+soggettoDescr2+"</x_250>"
    			+ "<c2_250>"+edizione+"</c2_250>"
    			+ "</T250>"
    			+ "</DatiElementoAut></ElementoAut></Modifica></SbnRequest></SbnMessage></SBNMarc>";
    	
    	String requestXml = requestXml_p1 + ute_var_sogg_polo + requestXml_p2 +userIdStr+ requestXml_p3 + livello_aut_sogg_in_polo + requestXml_p4;
    			
//System.out.println("Richiesta aggiornamento = " + requestXml);

    	
        // Effettuiamo l'aggiornamento
        response = eseguiRichiestaXml(requestXml);
        
		if (response.indexOf("<esito>0000</esito>") == -1)
		{
			
			boolean err3353Done=false;
			boolean err3010Done=false;
			// Gestione errori in cascata
			while (true)
			{
				if (response.indexOf("3353 Soggetto portato a questo livello da altro utente") > -1)
				{
					if (err3353Done == true)
						break; // errore gia' gestito precedentemente
					String response2 = gestioneSoggettoPortatoAQuestoLivelloDaAltroUtente(requestXml_p1, requestXml_p2, requestXml_p3, requestXml_p4, livelloAutoritaDB); 
					if (response2.isEmpty())
					{
						segnalaErrore(requestXml, response, livelloAutoritaDB);
						break;
					}
					
					if (response2.indexOf("<esito>0000</esito>") > -1)
						break;
					else
						err3353Done = true;
				}
				else if (response.indexOf("3010 Errore: il livello di autorit") > -1) // à sulla base dati è superiore
				{
					String response2 = gestioneLivelloAutoritaSuperioreInDb(requestXml_p1, requestXml_p2, requestXml_p3, requestXml_p4, livelloAutoritaDB);
					
					if (response2.isEmpty())
					{
						segnalaErrore(requestXml, response, livelloAutoritaDB);
						break;
					}

					if (response2.indexOf("<esito>0000</esito>") > -1)
						break;
					else
						err3010Done = true;
				}
				else
				{
					segnalaErrore(requestXml, response, livelloAutoritaDB);
					break;
				}
				if (err3353Done == true && err3010Done == true)
				{
//					try {
					String request = "\nRichiesta aggiornamento = " + requestXml;
						msg = "\n--\nErrore non gestito";
						log(msg);
						log(request);
						log("\n"+response);
						log("\n------");
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
					break;
				}
			}
			
//		System.out.println("--\nRichiesta aggiornamento = " + requestXml);
//		System.out.println("\nAggiornamento fallito per CID: "+cidStr+". Livello aut. indice="+livelloAutoritaDB+ ". Livello aut. polo="+livelloAutoritaPolo+ ". Risposta: "+response);
		return;
			
		}
	
//	System.out.println("Risposta aggiornamento: "+response);
    } // End ricerca ok
	
} // End elaboraRispostaRicercaSoggetto
	

void segnalaErrore(String requestXml, String response, String livelloAutoritaDB)
{
	String msg = "\nRichiesta aggiornamento = " + requestXml;
	log(msg);
	
	msg = "\nAggiornamento fallito per CID: "+cidStr
			+ ". ute_var_sogg_polo="+ute_var_sogg_polo
			+ ". ute_var_sogg_indice="+ ute_var_sogg_indice
			+ ". ute_var poloStr+bibliotecaStr="+poloStr+bibliotecaStr
			+ ". Livello aut. indice="+livelloAutoritaDB
			+ ". Livello aut. polo="+livelloAutoritaPolo
			+ ". Risposta: "+response;
	
	log(msg);
}




String gestioneLivelloAutoritaSuperioreInDb(String requestXml_p1, String requestXml_p2, String requestXml_p3, String requestXml_p4, String livelloAutoritaDB)
{
    String requestXml, response="";
    
    int livelloAutoritaDB_int = Integer.parseInt(livelloAutoritaDB);
    if (livelloAutoritaDB_int <= livelloAutoritaPolo_int) // controlliamo che il livello in indice non sia superiore a quello di polo
    {
        // Effettuiamo l'aggiornamento
        requestXml = requestXml_p1 + 
        			ute_var_sogg_indice + 
        			requestXml_p2 +userIdStr+ 
        			requestXml_p3 + 
        			livelloAutoritaDB + 
        			requestXml_p4;
        
        response = eseguiRichiestaXml(requestXml);
		if (response.indexOf("<esito>0000</esito>") > -1)
			return response;
		
//		try {
			msg = "\nRichiesta aggiornamento = " + requestXml;
			log(msg);

			msg =  ("\n\nAggiornamento fallito per CID: "+cidStr
					+ ". ute_var_sogg_polo="+ute_var_sogg_polo
					+ ". ute_var_sogg_indice="+ ute_var_sogg_indice
					+ ". ute_var poloStr+bibliotecaStr="+poloStr+bibliotecaStr
					+ ". Livello aut. indice="+livelloAutoritaDB
					+ ". Livello aut. polo="+livelloAutoritaPolo
					+ ". Risposta: "+response);
			log(msg);

		
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		
		return response;
    }
    else
    {
        requestXml = requestXml_p1 + ute_var_sogg_indice + requestXml_p2 +userIdStr+ requestXml_p3 + livelloAutoritaDB + requestXml_p4;
        response = eseguiRichiestaXml(requestXml);

		if (response.indexOf("<esito>0000</esito>") > -1)
			return response;
		
//		try {
			msg = "\nRichiesta aggiornamento = " + requestXml;
			log(msg);

	        msg = "\n\nLivello di autorita' insufficiente: "+cidStr
					+ ". ute_var_sogg_polo="+ute_var_sogg_polo
					+ ". ute_var_sogg_indice="+ ute_var_sogg_indice
					+ ". ute_var poloStr+bibliotecaStr="+poloStr+bibliotecaStr
					+ ". Livello aut. indice="+livelloAutoritaDB
					+ ". Livello aut. polo="+livelloAutoritaPolo
	        		+ ". Risposta: "+response;
			log(msg);
			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		

        return response;
    }

}			


String gestioneSoggettoPortatoAQuestoLivelloDaAltroUtente(String requestXml_p1, String requestXml_p2, String requestXml_p3, String requestXml_p4, String livelloAutoritaDB)
{
	String requestXml;
	String response = "";
	
	if (poloStr.equals("CFI"))
	{
		requestXml = requestXml_p1 + 
					ute_var_sogg_indice + 
					requestXml_p2 +
					userIdStr+ 
					requestXml_p3+ 
					livello_aut_sogg_in_polo + 
					requestXml_p4;

		//System.out.println("Richiesta aggiornamento = " + requestXml);
        // Effettuiamo l'aggiornamento
        response = eseguiRichiestaXml(requestXml);
    	if (response.indexOf("<esito>0000</esito>") == -1)
    		{
    		// Questo solo in caso di emergenza quando non posso ripristinare il DB di certifiazione per assenza unico sistemista in grado di ripristinare il DB
			requestXml = requestXml_p1 + 
					poloStr+bibliotecaStr + 
					requestXml_p2 +
					userIdStr+ 
					requestXml_p3+ 
					livello_aut_sogg_in_polo + 
					requestXml_p4;;
//System.out.println("Richiesta aggiornamento = " + requestXml);
	        response = eseguiRichiestaXml(requestXml);
	    	if (response.indexOf("<esito>0000</esito>") == -1)
	    	{
//	    		try {
		    		msg = "\n--\nRichiesta aggiornamento = " + requestXml;
					log(msg);

		    		msg = "\nAggiornamento fallito per CID: "+cidStr
		    				+ ". ute_var_sogg_polo="+ute_var_sogg_polo
		    				+ ". ute_var_sogg_indice="+ ute_var_sogg_indice
		    				+ ". ute_var poloStr+bibliotecaStr="+poloStr+bibliotecaStr
		    				+ ". Livello aut. indice="+livelloAutoritaDB
		    				+ ". Livello aut. polo="+livelloAutoritaPolo
		    				+ ". Risposta: "+response;
					log(msg);

//	    		} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
	    		
	    	}
		}
	}
return response;	
}
	
	
	
	
	
	
	


public void aggiornaSoggetto(String rigaSoggetto)
{
    //		System.out.println(rigaSoggetto);
	
	// CFIC165051^AFN ^ADanze popolari - Copenaghen - 1951-1962 - Fotografie
	
	String[] partiSoggetto = rigaSoggetto.split("\001");
	cidStr = partiSoggetto[0]; // CID POLO/INDICE
	edizione = partiSoggetto[1];
	livello_aut_sogg_in_polo = partiSoggetto[2]; //Integer.parseInt(partiSoggetto[2]); 
	ute_var_sogg_polo = partiSoggetto[3].substring(0, 6);
	
	partiSoggetto[4] = partiSoggetto[4].replace("<", "&lt;");	// Per evitare parsing errors
	partiSoggetto[4] = partiSoggetto[4].replace(">", "&gt;");
	descrittori = partiSoggetto[4].split(" - ");

	ute_var_sogg_indice = partiSoggetto[5].substring(0, 6);

//	msg = "\nAggiorna soggetto = " + cidStr;	
//	log(msg);
	
	// Costruisci la richiesta di ricerca
	String requestXml = "<?xml version='1.0' encoding='UTF-8'?>"
    		+ "<SBNMarc schemaVersion='"+schemaVersionStr+"'><SbnUser>"
    		+ "<Biblioteca>"
    		+ ute_var_sogg_polo	//poloStr+bibliotecaStr
    		+ "</Biblioteca>"
    		+ "<UserId>"
    		+ userIdStr
    		+ "</UserId></SbnUser><SbnMessage><SbnRequest>"
    		+ "<Cerca numPrimo='1' tipoOrd='2' tipoOutput='001'><CercaElementoAut>"
    		+ "<CercaDatiAut><tipoAuthority>SO</tipoAuthority><canaliCercaDatiAut>"
    		+ "<T001>"+cidStr+"</T001></canaliCercaDatiAut></CercaDatiAut></CercaElementoAut>"
    		+ "</Cerca></SbnRequest></SbnMessage></SBNMarc>"; 
           
//System.out.println("Richiesta ricerca = " + requestXml);
	// Esegui ricerca soggetto 
	elaboraRispostaRicercaSoggetto(eseguiRichiestaXml(requestXml));

}




public static void main(String[] args) {
	String start = "\nAggiornaSoggettiXml (c) ICCU 01/2017";

	if (args.length != 1)
	{
		System.out.println("AggiornaSoggetti fileDiConfigurazione");
		System.exit(1);
	}
	System.out.print(start);
	
	
	try {
		String fileSoggettiDaAggiornare="";
		String stringAr[];

		AggiornaSoggettiXml as = new AggiornaSoggettiXml();
		
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
			
		for(String line; (line = br.readLine()) != null; ) {
			if (line.isEmpty() || line.charAt(0) == '#')
				continue;
			stringAr = line.trim().split("=");
			if (stringAr[0].equals("fileSoggettiDaAggiornare"))
				fileSoggettiDaAggiornare=stringAr[1];
			else if (stringAr[0].equals("schemaVersionStr"))
				as.schemaVersionStr=stringAr[1];
			else if (stringAr[0].equals("poloStr"))
				as.poloStr=stringAr[1];
			else if (stringAr[0].equals("bibliotecaStr"))
				as.bibliotecaStr=stringAr[1];
			else if (stringAr[0].equals("userIdStr"))
				as.userIdStr=stringAr[1];
			else if (stringAr[0].equals("livelloAutoritaPolo"))
			{
				as.livelloAutoritaPolo=stringAr[1];
				as.livelloAutoritaPolo_int = Integer.parseInt(as.livelloAutoritaPolo);
			}
			else if (stringAr[0].equals("httpServer"))
				as.httpServer=stringAr[1];

			else if (stringAr[0].equals("servlet"))
				as.servlet=stringAr[1];
			else if (stringAr[0].equals("loginUid"))
				as.loginUid=stringAr[1];
			else if (stringAr[0].equals("loginPwd"))
				as.loginPwd=stringAr[1];
			else if (stringAr[0].equals("iniziaElaborazioneDaRiga"))
			{
				as.iniziaElaborazioneDaRigaStr = stringAr[1];
				as.iniziaElaborazioneDaRiga = Integer.parseInt(stringAr[1]);
			}
			else if (stringAr[0].equals("elaboraNRighe"))
			{
				as.elaboraNRigheStr = stringAr[1];
				as.elaboraNRighe = Integer.parseInt(stringAr[1]);
			}
			else if (stringAr[0].startsWith("progress"))
				as.progress = Integer.parseInt(stringAr[1], 16);
			
			else if (stringAr[0].startsWith("logFileOut"))
				as.logFileOut = stringAr[1];
			}
		
		String parametriInput = 
				"\nbibliotecaStr = '"+as.bibliotecaStr+"'"
				+"\nuserIdStr = '"+as.userIdStr+"'"
				+"\nlivelloAutoritaSoggettiPolo = '"+as.livelloAutoritaPolo+"'"
				+"\nhttpServer =  '"+as.httpServer+"'"
				+"\nservlet = '"+as.servlet+"'"
				+"\nloginUid = '"+as.loginUid+"'"
				+"\nloginPwd = '"+as.loginPwd+"'"
				+"\niniziaElaborazioneDaRiga = '"+as.iniziaElaborazioneDaRiga+"'"
				+"\nelaboraNRighe = '"+as.elaboraNRighe+"'"
				+"\n";
		
		
		// controlla che abbiamo tutti i parametri
		if (fileSoggettiDaAggiornare.isEmpty()
			|| as.schemaVersionStr.isEmpty()
			|| as.poloStr.isEmpty()
			|| as.bibliotecaStr.isEmpty()
			|| as.userIdStr.isEmpty()
			|| as.livelloAutoritaPolo.isEmpty()
			|| as.httpServer.isEmpty()
			|| as.servlet.isEmpty()
			|| as.loginUid.isEmpty()
			|| as.loginPwd.isEmpty()
			|| as.iniziaElaborazioneDaRigaStr.isEmpty()
			|| as.elaboraNRigheStr.isEmpty()
			
			)
		{
			System.out.println("Manca uno o piu parametri tra: " // , jdbcDriver, connectionUrl
					+parametriInput
					);
			System.exit(1);
		}

		System.out.println("Parametri da file:" 
				+parametriInput
				);

		
		// Apriamo il file di log
		try {
			System.out.println("File di log: "	+ as.logFileOut);
//			as.OutLog = new BufferedWriter(new FileWriter(as.logFileOut));				
			as.OutLog = new FileWriter(as.logFileOut);				
			as.log(start);
			
		} catch (Exception fnfEx) {
			fnfEx.printStackTrace();
			return;
		}
		
		as.openHttpConnection();
		
		br = new BufferedReader(new FileReader(fileSoggettiDaAggiornare));
		int ctr = 0;
		for(String line; (line = br.readLine()) != null; ) {
			if (line.length() > 0 && line.charAt(0) == '#')
				continue;
			ctr++;
			if (ctr < as.iniziaElaborazioneDaRiga)
				continue;
			
			as.aggiornaSoggetto(line);
			if (as.elaboraNRighe != 0 && ctr >= (as.iniziaElaborazioneDaRiga+as.elaboraNRighe))
				break;
			
//			if ((ctr & 0xFF) == 0)
//			if ((ctr & as.progress) == 0)
			if ((ctr % as.progress) == 0)
			{
				as.msg = "\nSoggetti elaborati:" +((ctr-as.iniziaElaborazioneDaRiga+1)+"\n");
				System.out.print(as.msg);
				as.log(as.msg);
			}
			} // End for

		
		as.msg = "\nSoggetti elaborati:" +((ctr-as.iniziaElaborazioneDaRiga+1)+"\n");
		System.out.print(as.msg);
		as.log(as.msg);
		as.OutLog.close();	// Chiudi log dopo aver caricato un file
		
		
	} catch (IOException e) {
		e.printStackTrace();
	}	
} // End main


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

//private void openConnection()
//{
//   try {
//        
//        url = new URL(httpServer);
//	    } // End try
//	    catch ( MalformedURLException ex ) {
//	        // a real program would need to handle this exception
//	    }
//	    catch ( IOException ex ) {
//	        // a real program would need to handle this exception
//	    }
//}

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
