package uploadSoggettoMT;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.logging.Logger;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnection;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PostMethod;

public class RunnableTask implements Runnable {
	  
//    private String command;
	private int lineNumber;
    private String soggetto_csv;
    private String msg;
    private String servlet;
    private Logger logger;
    
    private HttpConnection connection = null;
    HttpClient httpClient = null;
    
    public RunnableTask(MultiThreadedHttpConnectionManager multiThreadedHttpConnectionManager, int lineNumber, 
    		String soggetto_csv, String servlet, Logger logger, String httpServer, String loginUid, String loginPwd){
//        this.command=s;
    		this.lineNumber = lineNumber;
        	this.soggetto_csv = soggetto_csv;
        	this.servlet = servlet;
        	this.logger = logger;
    		
			httpClient = new HttpClient(multiThreadedHttpConnectionManager);
			httpClient.setHostConfiguration(new HostConfiguration());
			
		    httpClient.getState().setCredentials(
		    		new AuthScope(httpServer, 80, AuthScope.ANY_REALM),
		            new UsernamePasswordCredentials(loginUid, loginPwd)
		            
		            );
		    
			
			
    }

    @Override
    public void run() {
//        System.out.println(Thread.currentThread().getName()+" Start. Command = "+command);
//        String startMsg = Thread.currentThread().getName()+" START. line#="+lineNumber+" Soggetto = "+soggetto_csv;

//System.out.println(startMsg);
//logger.info(startMsg);
    	
        //
//        processCommand();
        uploadSoggetto(soggetto_csv);
        
//        System.out.println(Thread.currentThread().getName()+" End. Command = "+command);
//         String endMsg = Thread.currentThread().getName()+" END. line#="+lineNumber+" Soggetto = "+soggetto_csv;
//System.out.println(endMsg);
//logger.info(endMsg);
    }

    private void processCommand() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString(){
//        return this.command;
        return this.soggetto_csv;
    }
    
    
    
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
    	
    	
    	//CREA xml per SOGGETTO da caricare 
    	requestXml = 
    			"<?xml version='1.0' encoding='UTF-8'?><SBNMarc schemaVersion='2.01'><SbnUser>"
    			+ "<Biblioteca>"
    			+ uteVarInput.substring(0,6)	//poloStr+bibliotecaStr
    			+ "</Biblioteca>"
    			+ "<UserId>"
    			+ uteVarInput.substring(6)	//userIdStr
    			+ "</UserId></SbnUser>"
//    			+ "<SbnMessage><SbnRequest><Crea tipoControllo='Simile'><ElementoAut>"
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
      	
//    	String sendMsg = "Request per cid: "+cid+" "+uteVarInput+". "+requestXml;
    	//System.out.println(sendMsg);
String sendMsg = Thread.currentThread().getName()+" Request: line#="+lineNumber+" Soggetto = "+soggetto_csv;
logger.info(sendMsg);
    	
    	  	responseXml = eseguiRichiestaXml(requestXml);
    	  	if (responseXml.indexOf("<esito>0000</esito>") == -1)
    	  	{
    		  		String responseMsg = "\nRequest per cid: "+cid+" "+uteVarInput+". "+requestXml+
    		  				"\nInserimento soggetto fallito per cid: "+cid+" "+uteVarInput+". Response: "+responseXml;
logger.warning(responseMsg);
    	  		
//    	  		soggettiNonInseritiCtr++;
    	  	}
    	  	else
    	  	{
//    	  		soggettiInseritiCtr++;
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
//        post.addParameter("testo_xml", requestXmlEncoded); // MUST ENCODE else server misinterprets special characters
        
        
        
        int status=0;;
        try {
            // execute the POST
            status = httpClient.executeMethod( post );
            if (status != 200)
            	
//System.out.println("status: "+status);
logger.warning("status: "+status);
            
            else
            {
              BufferedReader in = new BufferedReader(new InputStreamReader(post.getResponseBodyAsStream(), "UTF-8"));
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
            post.releaseConnection(); // connection will go back to the MultiThreadedHttpConnectionManager connection pool
//            System.out.println("status: "+status);
        }
        response = sbResponse.toString();
//        System.out.println( "response = "+response);
        return response;
        
    }
    
}