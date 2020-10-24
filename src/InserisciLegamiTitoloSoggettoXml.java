/* 
 * Aggiorna soggetti in indice tramite protocollo SBNMARC
 * 
 * Argentino Trombin
 * 07/12/2016
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PostMethod;

public class InserisciLegamiTitoloSoggettoXml {
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
	
    private HttpClient httpClient = null;
    
   
    private BufferedReader in;
    
	int legamiDaInserireCtr=0;
	int legamiInseritiCtr=0;
	int legamiNonInseritiCtr=0;
	int legamiGiaEsistentiCtr=0;
	
	int rigaLegamiSoggettoElaboratiCtr=0;

public void inserisciLegamiTitoloSoggetto(String cid, HashMap<String, String> legamiMap)    
{
    String timeStamp;
    String requestXml;
	String responseXml;

    
    // Con il BID, TIMESTAMP del bid ed il CID creiamo il legame
//    for (int i=1; i < legamiMap.size(); i++)
    	
	Iterator entries = legamiMap.entrySet().iterator();
	while (entries.hasNext()) {
	  Entry thisEntry = (Entry) entries.next();
	  String bid = (String)thisEntry.getKey();
	  String cdPolo = ((String)thisEntry.getValue()).substring(0, 3);
	  String cdBiblioteca = ((String)thisEntry.getValue()).substring(4);
//System.out.println("BID="+bid);
//System.out.println("cd polo="+cdPolo);
//System.out.println("cd biblioteca="+cdBiblioteca);

//  	legamiDaInserireCtr++;
	  
  	// Andiamo a prendere il timestamp del titolo da legare
  	requestXml = "<?xml version='1.0' encoding='UTF-8'?>"
  			+ "<SBNMarc schemaVersion='2.01'><SbnUser><Biblioteca>XXXAMM</Biblioteca>"
  			+ "<UserId>"
  			+ "000001"
  			+ "</UserId></SbnUser><SbnMessage><SbnRequest>"
  			+ "<Cerca numPrimo='1' tipoOrd='1' tipoOutput='000'><CercaTitolo><CercaDatiTit><T001>"
  			+ bid
  			+ "</T001></CercaDatiTit></CercaTitolo></Cerca></SbnRequest></SbnMessage></SBNMarc>";
  	responseXml = eseguiRichiestaXml(requestXml);

  	if (responseXml.indexOf("<esito>0000</esito>") == -1)
  	{
  		System.out.println("Ricerca fallita per BID: "+bid+". Risposta: "+responseXml);
  		legamiNonInseritiCtr++;
  		continue;
  	}
  	// Prendiamo il timestamp
  	int tsStart = responseXml.indexOf("<T005>");
  	int tsEnd = responseXml.indexOf("</T005>");
  	if (tsStart == -1 || tsEnd == -1)
  	{
        System.out.println("Non trovo il timestamp per bid " + bid );
		legamiNonInseritiCtr++;
		continue;
  	}

	timeStamp = responseXml.substring(tsStart+"<T005>".length(), tsEnd); 
  	
  	// Con il CID, BID e TIMESTAMP inseriamo il legame
  	requestXml = "<?xml version='1.0' encoding='UTF-8'?><SBNMarc schemaVersion='2.01'><SbnUser><Biblioteca>"
  			+ cdPolo+cdBiblioteca
  			+ "</Biblioteca><UserId>"
  			+ userIdStr
  			+ "</UserId></SbnUser><SbnMessage><SbnRequest><Modifica tipoControllo='Simile'>"
  			+ "<Documento><DatiDocumento tipoMateriale='M' livelloAutDoc='51' naturaDoc='M' "
  			+ "xsi:type='ModernoType' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>"
  			+ "<guida tipoRecord='a'/><T001>"
  			+ bid
  			+ "</T001><T005>"
  			+ timeStamp
  			+ "</T005></DatiDocumento>"
  			+ "<LegamiDocumento tipoOperazione='Crea'><idPartenza>"
  			+ bid
  			+ "</idPartenza><ArrivoLegame><LegameElementoAut tipoAuthority='SO' tipoLegame='606'><idArrivo>"
  			+ cid
  			+ "</idArrivo></LegameElementoAut></ArrivoLegame></LegamiDocumento></Documento></Modifica>"
  			+ "</SbnRequest></SbnMessage></SBNMarc>";  	
  	
	  	responseXml = eseguiRichiestaXml(requestXml);
	  	if (responseXml.indexOf("<esito>0000</esito>") == -1)
	  	{
	  		System.out.println("Inserimento legame fallito: "+cid+" -> "+bid+" "+cdPolo+cdBiblioteca+". Response: "+responseXml);
	  		legamiNonInseritiCtr++;
	  	}
	  	else
	  	{
//	  		System.out.println("Inserito legame: "+cid+" -> "+bid+" "+cdPolo+cdBiblioteca);
	  		legamiInseritiCtr++;
	  	}

	  	
	} // end while    	
} // End inserisciLegamiTitoloSoggetto



public void cercaLegamiTitoloSoggetto(String rigaLegamiSoggettoTitoli)
{
//	System.out.println("Elaborando legami a: " + rigaLegamiSoggettoTitoli );

	String [] legamiSoggetto = rigaLegamiSoggettoTitoli.split("\\|");
	//ArrayList<String> legamiSoggettoList = new ArrayList<String>(Arrays.asList(legamiSoggetto)); 
	
	
	HashMap<String, String> legamiSoggettoMap = new HashMap<String, String>();
	for (int i=1; i < legamiSoggetto.length; i++)
		legamiSoggettoMap.put(legamiSoggetto[i].substring(0, 10), legamiSoggetto[i].substring(11));

	legamiDaInserireCtr += legamiSoggettoMap.size(); 
	
//	ArrayList<String> legamiTitoloSoggettoNuovi = new ArrayList<String>();
	
	// Troviamo tutti i legami per il soggetto gia' presenti in indice

	// Costruisci la richiesta di ricerca
	String requestXml = "<?xml version='1.0' encoding='UTF-8'?> "
    		+ "<SBNMarc schemaVersion='2.01'>"
    		+ "<SbnUser>"
    		+ "<Biblioteca>XXXAMM</Biblioteca>"
    		+ "<UserId>"
    		+ "000001"
    		+ "</UserId>"
    		+ "</SbnUser>"
    		+ "<SbnMessage>"
    		+ "<SbnRequest>"
    		+ "<Cerca numPrimo='1' tipoOrd='2' tipoOutput='003'>"
    		+ "<CercaTitolo>"
    		+ "<CercaDatiTit/>"
    		+ "<ArrivoLegame>"
    		+ "<LegameElementoAut tipoAuthority='SO' tipoLegame='tutti'>"
    		+ "<idArrivo>"+legamiSoggetto[0]+"</idArrivo>"
//    		+ "<idArrivo>IEIC107942</idArrivo>"
    		+ "</LegameElementoAut>"
    		+ "</ArrivoLegame>"
    		+ "</CercaTitolo>"
    		+ "</Cerca>"
    		+ "</SbnRequest>"
    		+ "</SbnMessage>"
    		+ "</SBNMarc>";
    		
	String response = eseguiRichiestaXml(requestXml);
//	System.out.println("Response:"+response);
		
	if (response.contains("<esito>3001</esito>"))
	{	// Non esiste alcun legame. Sono tutti nuovi
		
//		System.out.println("Soggetto "+legamiSoggetto[0]+" non ha alcun legame. Tutti nuovi");
		// Lista dei legami da inserire gia' pronta
		
	}	
	else
	{	// Esistono gia' dei legami

		String [] legamiTitolo = response.split("<guida");
		
		for (int i=1; i < legamiTitolo.length; i++)
		{
			int idx_001 = legamiTitolo[i].indexOf("<T001>");
			String bid = legamiTitolo[i].substring(idx_001+6, idx_001+6+10);
			
			// Eliminiamo dalla lista dei bid da legare i bid gia' legati
			if (legamiSoggettoMap.containsKey(bid))
			{
				legamiSoggettoMap.remove(bid);
				legamiGiaEsistentiCtr++;
			}

//			System.out.println("Bid "+bid);
		}
		
	}
	
	if (legamiSoggettoMap.size() > 0)
		inserisciLegamiTitoloSoggetto(legamiSoggetto[0], legamiSoggettoMap);
//	else
//		System.out.println("Nessun legame nuovo per cid: "+legamiSoggetto[0]);
	
	
}

public static void main(String[] args) {
//	System.out.println("Hello aggiorna soggetti - 05/12/2016");

	if (args.length != 1)
	{
		System.out.println("InserisciLegamiTitoloSoggettoXml fileDiConfigurazione");
		System.exit(1);
	}
	System.out.println("InserisciLegamiTitoloSoggettoXml (c) ICCU 01/2017");

	
	try {
		String fileLegamiSoggettoTitoli="";
		String stringAr[];

		InserisciLegamiTitoloSoggettoXml ilts = new InserisciLegamiTitoloSoggettoXml();
		
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
			
		for(String line; (line = br.readLine()) != null; ) {
			if (line.isEmpty() || line.charAt(0) == '#')
				continue;
			stringAr = line.trim().split("=");
			if (stringAr[0].equals("fileLegamiSoggettoTitoli"))
				fileLegamiSoggettoTitoli=stringAr[1];
			if (stringAr[0].equals("schemaVersionStr"))
				ilts.schemaVersionStr=stringAr[1];
			else if (stringAr[0].equals("poloStr"))
				ilts.poloStr=stringAr[1];
			else if (stringAr[0].equals("bibliotecaStr"))
				ilts.bibliotecaStr=stringAr[1];
			else if (stringAr[0].equals("userIdStr"))
				ilts.userIdStr=stringAr[1];
			else if (stringAr[0].equals("livelloAutoritaSoggettiPolo"))
				ilts.livelloAutoritaSoggettiPolo=stringAr[1];
			else if (stringAr[0].equals("httpServer"))
				ilts.httpServer=stringAr[1];
			else if (stringAr[0].equals("servlet"))
				ilts.servlet=stringAr[1];

			else if (stringAr[0].equals("loginUid"))
				ilts.loginUid=stringAr[1];
			else if (stringAr[0].equals("loginPwd"))
				ilts.loginPwd=stringAr[1];
			else if (stringAr[0].equals("iniziaElaborazioneDaRiga"))
			{
				ilts.iniziaElaborazioneDaRigaStr = stringAr[1];
				ilts.iniziaElaborazioneDaRiga = Integer.parseInt(stringAr[1]);
			}
			else if (stringAr[0].equals("elaboraNRighe"))
			{
				ilts.elaboraNRigheStr = stringAr[1];
				ilts.elaboraNRighe = Integer.parseInt(stringAr[1]);
			}
		
		}

		String parametriInput = 
				"\nbibliotecaStr = '"+ilts.bibliotecaStr+"'"
				+"\nuserIdStr = '"+ilts.userIdStr+"'"
				+"\nlivelloAutoritaSoggettiPolo = '"+ilts.livelloAutoritaSoggettiPolo+"'"
				+"\nhttpServer =  '"+ilts.httpServer+"'"
				+"\nservlet = '"+ilts.servlet+"'"
				+"\nloginUid = '"+ilts.loginUid+"'"
				+"\nloginPwd = '"+ilts.loginPwd+"'"
				+"\niniziaElaborazioneDaRiga = '"+ilts.iniziaElaborazioneDaRiga+"'"
				+"\nelaboraNRighe = '"+ilts.elaboraNRighe+"'"
				+"\n";

		
		// controlla che abbiamo tutti i parametri
		if (fileLegamiSoggettoTitoli.isEmpty()
			|| ilts.schemaVersionStr.isEmpty()
			|| ilts.poloStr.isEmpty()
			|| ilts.bibliotecaStr.isEmpty()
			|| ilts.userIdStr.isEmpty()
			|| ilts.livelloAutoritaSoggettiPolo.isEmpty()
			|| ilts.servlet.isEmpty()
			|| ilts.loginUid.isEmpty()
			|| ilts.loginPwd.isEmpty()
			|| ilts.httpServer.isEmpty()
			|| ilts.iniziaElaborazioneDaRigaStr.isEmpty()
			|| ilts.elaboraNRigheStr.isEmpty()
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
		
		ilts.openHttpConnection();
		
		br = new BufferedReader(new FileReader(fileLegamiSoggettoTitoli));
		int ctr = 0;
		for(String line; (line = br.readLine()) != null; ) {
			if (line.trim().length() > 0 && line.charAt(0) == '#')
				continue;
			ctr++;
			if (ctr < ilts.iniziaElaborazioneDaRiga)
				continue;
			
			
			ilts.cercaLegamiTitoloSoggetto(line);
			if (ilts.elaboraNRighe != 0 && ctr >= (ilts.iniziaElaborazioneDaRiga+ilts.elaboraNRighe))
				break;
			
			if ((ctr & 0x0F) == 0)
				System.out.println("Soggetti elaborati (righe):" + (ctr-ilts.iniziaElaborazioneDaRiga+1));

		
		}
		System.out.println("Inizio inserimento da riga:" +ilts.iniziaElaborazioneDaRiga);
		System.out.println("Legami da inserire:" +ilts.legamiDaInserireCtr);
		System.out.println("Legami gia' esistenti:" +ilts.legamiGiaEsistentiCtr);
		System.out.println("Legami inseriti:" +ilts.legamiInseritiCtr);
		System.out.println("Legami non inseriti:" +ilts.legamiNonInseritiCtr);
		
		
		
	} catch (IOException e) {
		e.printStackTrace();
	}	
} // End main





private String eseguiRichiestaXml(String requestXml)
{
    String response="";
    StringBuffer sbResponse = new StringBuffer();
	
    PostMethod post = new PostMethod(servlet); // "http://10.30.1.83/certificazione/servlet/serversbnmarc"
    post.setDoAuthentication( true );
    post.addParameter("testo_xml", requestXml);
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

}
