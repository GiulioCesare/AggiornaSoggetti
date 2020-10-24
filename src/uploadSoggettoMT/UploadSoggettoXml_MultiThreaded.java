/* 
 * Aggiorna soggetti in indice tramite protocollo SBNMARC con multithreading 
 * Argentino Trombin
 * 10/11/2017
 * 
 * DEV:
 * 	/media/export54/indice/dp/soggetti/rml/uploadSoggettiXml_MT_run.cfg
 * 
 */
package uploadSoggettoMT;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;

import it.finsiel.misc.DateUtil;

import uploadSoggettoMT.RejectedExecutionHandlerImpl;;

public class UploadSoggettoXml_MultiThreaded {
	private String fileSoggettiDaCaricare="";
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

	
	
	private int threads; // Number of concurrent threads
	private int queueTaskWaitSeconds; // Tempo di attesa in secondi per vedere se si e' liberato un posto nella coda per inserire nuova richiesta

	// Defaults for multi-threading
	private int core_pool_size = 4;
	private int maximum_pool_size = 4;
	private int keep_alive_seconds = 5;  
	private int maximum_queue_size = maximum_pool_size;
	private int execute_wait_seconds = 1; // time to wait to see if the queue has freed up some space  
	private int monitor_wait_seconds = 5;

	int HTTP_CLIENT_NUMBER_OF_THREADS = maximum_pool_size;	
	
	String logFileOut = "uploadSoggetto_MultiThreaded.log"; // default
//	FileWriter  OutLog;  
	Logger logger= null;  

	
	int progress=0xff; 
    String msg;
	
   // private HttpClient httpClient = null;
   
    MultiThreadedHttpConnectionManager multiThreadedHttpConnectionManager;
    
    
    
	int soggettiElaboratiCtr=0;
	int soggettiInseritiCtr=0;
	int soggettiNonInseritiCtr=0;
	int soggettiGiaEsistentiCtr=0;
	












private void openHttpConnection()
{
/*	
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
       
*/
	multiThreadedHttpConnectionManager = new MultiThreadedHttpConnectionManager();
	
	HttpConnectionManagerParams httpConnectionManagerParams = multiThreadedHttpConnectionManager.getParams();
	httpConnectionManagerParams.setMaxConnectionsPerHost(HostConfiguration.ANY_HOST_CONFIGURATION, HTTP_CLIENT_NUMBER_OF_THREADS);
	multiThreadedHttpConnectionManager.setParams(httpConnectionManagerParams);
	
}

private void init(String[] args)
{
	String stringAr[];

	try {
	
	BufferedReader br = new BufferedReader(new FileReader(args[0]));
		
	for(String line; (line = br.readLine()) != null; ) {
		if (line.isEmpty() || line.charAt(0) == '#')
			continue;
		stringAr = line.trim().split("=");
		if (stringAr[0].equals("fileSoggettiDaCaricare"))
			fileSoggettiDaCaricare=stringAr[1];
		if (stringAr[0].equals("schemaVersionStr"))
			this.schemaVersionStr=stringAr[1];
		else if (stringAr[0].equals("poloStr"))
			this.poloStr=stringAr[1];
		else if (stringAr[0].equals("bibliotecaStr"))
			this.bibliotecaStr=stringAr[1];
		else if (stringAr[0].equals("userIdStr"))
			this.userIdStr=stringAr[1];
		else if (stringAr[0].equals("livelloAutoritaSoggettiPolo"))
			this.livelloAutoritaSoggettiPolo=stringAr[1];
		else if (stringAr[0].equals("httpServer"))
			this.httpServer=stringAr[1];
		else if (stringAr[0].equals("servlet"))
			this.servlet=stringAr[1];
		else if (stringAr[0].equals("loginUid"))
			this.loginUid=stringAr[1];
		else if (stringAr[0].equals("loginPwd"))
			this.loginPwd=stringAr[1];
		else if (stringAr[0].equals("iniziaElaborazioneDaRiga"))
		{
			this.iniziaElaborazioneDaRigaStr = stringAr[1];
			this.iniziaElaborazioneDaRiga = Integer.parseInt(stringAr[1]);
		}
		else if (stringAr[0].equals("elaboraNRighe"))
		{
			this.elaboraNRigheStr = stringAr[1];
			this.elaboraNRighe = Integer.parseInt(stringAr[1]);
		}
		else if (stringAr[0].startsWith("logFileOut"))
			this.logFileOut = stringAr[1];

		else if (stringAr[0].startsWith("progress"))
			this.progress = Integer.parseInt(stringAr[1], 16);
		
		else if (stringAr[0].startsWith("threads"))
		{
			this.threads = Integer.parseInt(stringAr[1]);
			this.core_pool_size = this.threads;
			this.maximum_pool_size = this.threads;
			this.HTTP_CLIENT_NUMBER_OF_THREADS = this.threads; 
		}	
		else if (stringAr[0].startsWith("queueTaskWaitSeconds"))
			this.queueTaskWaitSeconds = Integer.parseInt(stringAr[1]);
		this.execute_wait_seconds = this.queueTaskWaitSeconds;
		}

	
	
	String parametriInput = 
			
			"\npoloStr = '"+this.poloStr+"'"
			+"\nbibliotecaStr = '"+this.bibliotecaStr+"'"
			+"\nuserIdStr = '"+this.userIdStr+"'"
			+"\nlivelloAutoritaSoggettiPolo = '"+this.livelloAutoritaSoggettiPolo+"'"
			+"\nhttpServer =  '"+this.httpServer+"'"
			+"\nservlet = '"+this.servlet+"'"
			+"\nloginUid = '"+this.loginUid+"'"
			+"\nloginPwd = '"+this.loginPwd+"'"
			+"\niniziaElaborazioneDaRiga = '"+this.iniziaElaborazioneDaRiga+"'"
			+"\nelaboraNRighe = '"+this.elaboraNRighe+"'"
			+"\nthreads = '"+this.threads+"'"
			+"\nqueueTaskWaitSeconds = '"+this.queueTaskWaitSeconds+"'"
			+"\n";
	
	
	// controlla che abbiamo tutti i parametri
	if (fileSoggettiDaCaricare.isEmpty()
		|| this.schemaVersionStr.isEmpty()
		|| this.poloStr.isEmpty()
		|| this.bibliotecaStr.isEmpty()
		|| this.userIdStr.isEmpty()
		|| this.livelloAutoritaSoggettiPolo.isEmpty()
		|| this.servlet.isEmpty()
		|| this.loginUid.isEmpty()
		|| this.loginPwd.isEmpty()
		|| this.httpServer.isEmpty()
		|| this.iniziaElaborazioneDaRigaStr.isEmpty()
		|| this.elaboraNRigheStr.isEmpty()
		
		)
	{
		System.out.println("Manca uno o piu parametri tra: " // , jdbcDriver, connectionUrl
				+parametriInput
				);
		System.exit(1);
	}
	
	// Apriamo il file di log
	String start = "UploadSoggettoXml_MultiThreaded (c) ICCU 11/2017";
//	try {
//		System.out.println("File di log: "	+ this.logFileOut);
//		this.OutLog = new FileWriter(this.logFileOut);				
//		this.log(start);
//		this.log("Parametri da file: "+parametriInput);
//		System.out.print(start);
//	} catch (IOException e) {
//		e.printStackTrace();
//	}	

    logger = Logger.getLogger(UploadSoggettoXml_MultiThreaded.class.getName());  
    FileHandler fh;  
    try {  

        // This block configure the logger with handler and formatter  
        fh = new FileHandler(this.logFileOut);  
        logger.addHandler(fh);
        
        SimpleFormatter formatter = new SimpleFormatter();
//        formatter.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");
        
        logger.setUseParentHandlers(false); // Remove the console handler
        
        // Customized formatter to go on a single line 
        fh.setFormatter(new Formatter() {
	  	  	DateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
	  	  	public String format(LogRecord record) {
	          return record.getLevel() + "  :  "
	        	  + formatter.format(new Date(record.getMillis())) + " | "
//	              + record.getSourceClassName() + " | "
//	              + record.getSourceMethodName() + " | "
	              + record.getMessage() + "\n";
	        }
          });
        
        System.out.println(start);
    	logger.info(start);
    	logger.info("Parametri da file: "+parametriInput);

    
    } catch (SecurityException e) {  
        e.printStackTrace();  
    } catch (IOException e) {  
        e.printStackTrace();  
    }  
	} catch (Exception fnfEx) {
	fnfEx.printStackTrace();
	return;
}

	
	
	
} // End init

//void log(String s)
//{
//	try {
//		OutLog.write(s);
//		OutLog.flush();
//	} catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//} // End log


public static void main(String[] args) {
//	System.out.println("Hello aggiorna soggetti - 05/12/2016");
	
	 
	
	if (args.length != 1)
	{
		System.out.println("UploadSoggettoXml_MultiThreaded fileDiConfigurazione");
		System.exit(1);
	}

	// Disable http client logging that uses commons logging
	System.setProperty("org.apache.commons.logging.Log",
            "org.apache.commons.logging.impl.NoOpLog");


	
	UploadSoggettoXml_MultiThreaded uploadSoggettoXml_MT = new UploadSoggettoXml_MultiThreaded();

	uploadSoggettoXml_MT.init(args); 

	
		uploadSoggettoXml_MT.openHttpConnection();
		
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(uploadSoggettoXml_MT.fileSoggettiDaCaricare));
		int ctr = 0;

    	//RejectedExecutionHandler implementation
        RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();
        //Get the ThreadFactory implementation to use
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        //creating the ThreadPoolExecutor
        ThreadPoolExecutor executorPool = new ThreadPoolExecutor(uploadSoggettoXml_MT.core_pool_size, 
        		uploadSoggettoXml_MT.maximum_pool_size, uploadSoggettoXml_MT.keep_alive_seconds, 
        		TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(uploadSoggettoXml_MT.maximum_queue_size), threadFactory, rejectionHandler);
		
		int lineNumber=0;
		for(String soggetto_csv; (soggetto_csv = br.readLine()) != null; ) {
			lineNumber++;
			if (soggetto_csv.trim().length() > 0 && soggetto_csv.charAt(0) == '#')
				continue;
			ctr++;
			
			if (ctr < uploadSoggettoXml_MT.iniziaElaborazioneDaRiga)
				continue;
/*				
			if ((ctr & uploadSoggettoXml_MT.progress) == 0)
			{
				uploadSoggettoXml_MT.msg = "\nSoggetti elaborati:" +(ctr-uploadSoggettoXml_MT.iniziaElaborazioneDaRiga+1);
				System.out.print(uploadSoggettoXml_MT.msg);
//				uploadSoggettoXml_MultiThreaded.log(uploadSoggettoXml_MultiThreaded.msg);
				
			}
*/			
	
        	// Can we submit another task without being rejected?
            while (executorPool.getQueue().remainingCapacity() == 0) // is the task queue full? 
            { // wait till we have a free slot for a new task
                try {
                    Thread.sleep(uploadSoggettoXml_MT.execute_wait_seconds*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            	
            }
//            queueRemainingCapacity = executorPool.getQueue().remainingCapacity();
//            System.out.println( String.format("[main loop] queue remaining capacity %d", queueRemainingCapacity));

            executorPool.execute(new RunnableTask(uploadSoggettoXml_MT.multiThreadedHttpConnectionManager, 
            		lineNumber, soggetto_csv, 
            		uploadSoggettoXml_MT.servlet, uploadSoggettoXml_MT.logger, uploadSoggettoXml_MT.httpServer,
            		uploadSoggettoXml_MT.loginUid, uploadSoggettoXml_MT.loginPwd));
            
			uploadSoggettoXml_MT.soggettiElaboratiCtr++;	
			if (uploadSoggettoXml_MT.elaboraNRighe != 0 && ctr >= (uploadSoggettoXml_MT.iniziaElaborazioneDaRiga + uploadSoggettoXml_MT.elaboraNRighe -1))
				break;
			

//			if ((ctr & 0x3F) == 0)
			if ((uploadSoggettoXml_MT.soggettiElaboratiCtr & uploadSoggettoXml_MT.progress) == 0)
				System.out.println("Soggetti accodati per elaborazione :" +(uploadSoggettoXml_MT.soggettiElaboratiCtr));
		
		
		} // End for processing messages 
		
        while (executorPool.getActiveCount() != 0) 
        { 
            try {
                System.out.println("waiting to shut down the pool");
                Thread.sleep(1*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        	
        }
        System.out.println("shutting down the pool");
        executorPool.shutdown();
		
		
		uploadSoggettoXml_MT.msg = "\nInizio inserimento da riga:" +uploadSoggettoXml_MT.iniziaElaborazioneDaRiga + 
				"\nSoggetti elaborati:" +uploadSoggettoXml_MT.soggettiElaboratiCtr + 
//				"\nSoggetti gia' esistenti:" +uploadSoggettoXml_MT.soggettiGiaEsistentiCtr+
//				"\nSoggetti inseriti:" +uploadSoggettoXml_MT.soggettiInseritiCtr+
//				"\nSoggetti non inseriti:" +uploadSoggettoXml_MT.soggettiNonInseritiCtr+ 
				"\n" + DateUtil.getDate() + " " + DateUtil.getTime()
				;
//		uploadSoggettoXml_MultiThreaded.log(uploadSoggettoXml_MultiThreaded.msg  );
		
		System.out.print(uploadSoggettoXml_MT.msg);
		uploadSoggettoXml_MT.logger.info(uploadSoggettoXml_MT.msg);
		
		
		
//		uploadSoggettoXml_MultiThreaded.OutLog.close();	// Chiudi log dopo aver caricato un file
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
} // End main


}
