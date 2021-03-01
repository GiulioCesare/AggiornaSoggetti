/**

Batch per inserimento legami titolo soggetto

/media/export54/indice/dp/cfi_soggetti/inserisciLegamiTitoloSoggettoJdbc.txt

**/

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import it.finsiel.misc.Misc;

public class InserisciLegamiTitoloSoggettoJdbc {



/*
 Controlli sulle priorita' di inserimento legame tra polo operante ed ute_var del legame esistente (DB)
Legame che si trova nella tr_tit_sog  

Prendo i primi tre caratteri di UTE_VAR per trovare il polo

 	
A.  SE ute_var e' un polo soggettatore (dalla tabellina dei poli soggettatori)
    confronta le priorita' tra i due utenti: 
    	se ute_var ha priorita' maggiore non permette la modifica
    	se ute_var ha priorita' Minore permette la modifica
    	

B.  SE ute_var non e' un polo soggettatore (non in tabellina)
	    se ute_var e' un utente di ID non permette la modifica 
	    se ute_var NON e' un utente di ID permette la modifica 
    

quando la modifica dei legami è ammissibile
se utente operante e ute_var sono diversi si cancellano TUTTI i legami del titolo con i soggetti,
altrimenti si aggiunge il legame a quelli esistenti


// POLI SOGGETTATORI
CFI9090
MIL8585
RAV8080
LO17070
BVE6060
VEA5050
PUV4040
UFI3030
UBO2525
NAP2020
IEI1010
	
*/	
	


	
	public static final int MAX_BYTES_PER_UTF8_CHARACTER = 4;

//	final int TB_SOGGETTO = 1;
	
	
//	Map prioritaMap = new HashMap();    // hash table
	HashMap<String, Integer> prioritaMap = new HashMap<String, Integer>();

//	Map parameters = new HashMap();    // hash table

	ArrayList sqlExecutedOk = new ArrayList();
	int rollbackRecordsFrom = 1;
	
	
	java.util.Vector vecInputFiles; // Buffer Reader Files
	String downloadDir = "";
	int fileCounter = 0;
	String logFileOut = "";
	String fileOut = "";
	boolean setCurCfg=false;
	//FileOutputStream streamOutLog;
	//BufferedWriter OutLog;
	
	BufferedOutputStream bufferedStreamOutTable;	// 15/09/2010
	BufferedOutputStream bufferedStreamOutTableBytes;

	
	Connection con = null;
	int commitOgniTotRighe = 10;
	String jdbcDriver="";
	String connectionUrl = ""; // "jdbc:postgresql://localhost:5432/sbnwebArge"
	String userName="";
	String userPassword="";
	int progress=0x3ff; 
	String tr_tit_sog_rel;
	String tb_gerarchia_soggetti;
	String poloStr;
	String bibliotecaStr;
	int priorita_polo_operante;
	int iniziaElaborazioneDaRiga;
	int elaboraNRighe;
	
	
	String fileIdDaRimuovere = null;
	String query;
	String preprocess;

	int legamiInseriti = 0;
	int legamiNonInseriti = 0;
//	int legamiAggiornati = 0;
	int titoli_da_legare=0;
	int cancellaTuttiILegami=0;
	
	public InserisciLegamiTitoloSoggettoJdbc()
	    {
	    }

	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
	
	public boolean openConnection()
	{
		System.out.println("Conncecting to " + connectionUrl + " userName=" + userName);
		
		try {
			 Class.forName ( jdbcDriver);
		} catch(java.lang.ClassNotFoundException e) {
			System.err.print("ClassNotFoundException: ");
			System.err.println(e.getMessage());
		}

		try {

				con = DriverManager.getConnection(connectionUrl,userName, userPassword);
				boolean autoCommit = con.getAutoCommit();
				con.setAutoCommit(false);
				
				Statement stmt = null;
				try {
					stmt = con.createStatement();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// if (jdbcDriver.contains("postgres"))
				int pos = jdbcDriver.indexOf("postgres"); // 09/12/2010
				if (pos  != -1)
				{
					stmt.execute("SET search_path = sbnweb, pg_catalog, public");
					if (setCurCfg == true)
						stmt.execute("select set_curcfg('default')"); // Esegui questa select per attivare gli indici testuali 
				}
				
				return true;
		} catch(SQLException ex) {
			System.err.println("SQLException: " + ex.getMessage());
		}
		return false;
	} // End openConnection
	

	public void closeConnection()
	{
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} // End closeConnection
	
String[] getPropertyKeyValue(String s)
{
	int pos = s.indexOf("=");
	if (pos == -1)
	{
		String[] arrCampi = new String[1];
		arrCampi[0] = s;
		return arrCampi;
	}
		
	
	String key = new String (s.substring(0,pos));
	String value = new String (s.substring(pos+1));
	String[] arrCampi = new String[2];

	arrCampi[0] = key;
	arrCampi[1] = value;
	return arrCampi;

} // End getPropertyKeyValue



/*
private byte[] myGetBytesUtf8(String s) {
	int len = s.length();
	int en = MAX_BYTES_PER_UTF8_CHARACTER * len;
	byte[] ba = new byte[en];
	if (len == 0)
		return ba;

	int ctr = 0;

	for (int i = 0; i < len; i++) {
		char c = s.charAt(i);
		if (c < 0x80) {
			ba[ctr++] = (byte) c;
		} else if (c < 0x800) {
			ba[ctr++] = (byte) (0xC0 | c >> 6);
			ba[ctr++] = (byte) (0x80 | c & 0x3F);
		} else if (c < 0x10000) {
			ba[ctr++] = (byte) (0xE0 | c >> 12);
			ba[ctr++] = (byte) (0x80 | c >> 6 & 0x3F);
			ba[ctr++] = (byte) (0x80 | c & 0x3F);
		} else if (c < 0x200000) {
			ba[ctr++] = (byte) (0xE0 | c >> 18);
			ba[ctr++] = (byte) (0x80 | c >> 12 & 0x3F);
			ba[ctr++] = (byte) (0x80 | c >> 6 & 0x3F);
			ba[ctr++] = (byte) (0x80 | c & 0x3F);
		} else if (c < 0x800) {

		}
	} // end for

	return trim(ba, ctr);
} // End myGetBytesUtf8

private static byte[] trim(byte[] ba, int len) {
	if (len == ba.length)
		return ba;
	byte[] tba = new byte[len];
	System.arraycopy(ba, 0, tba, 0, len);
	return tba;
}

*/


//private void caricaGerarchieVecchie()
//{
//	prioritaMap.put("CFI", 90);
//	prioritaMap.put("MIL", 85);
//	prioritaMap.put("RAV", 80);
//	prioritaMap.put("LO1", 70);
//	prioritaMap.put("BVE", 60);
//	prioritaMap.put("VEA", 50);
//	prioritaMap.put("PUV", 40);
//	prioritaMap.put("UFI", 30);
//	prioritaMap.put("UBO", 25);
//	prioritaMap.put("NAP", 20);
//	prioritaMap.put("IEI", 10);
//	
//}


private void caricaGerarchieNuove()
{
	BufferedReader in;
	String s;

	try {
		in = new BufferedReader(new InputStreamReader(new FileInputStream(tb_gerarchia_soggetti),"UTF8"));
		String key;
		int value;
		// Leggiamo le configurazioni di base
		// ----------------------------------
		while (true) {
			try {
				s = in.readLine();
				if (s == null)
					break;
				else if (Misc.emptyString(s))
					continue;
				else if ((	s.length() == 0) 
							||  (s.charAt(0) == '#') 
							|| (Misc.emptyString(s) == true))
						continue;

				key = s.substring(0,6); 
				value = Integer.parseInt(s.substring(6,8));

				prioritaMap.put(key, value);
			
			
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
			}
		}
	 catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		System.exit(1);
	}
}


private void caricaGerarchie()
{
	
//	caricaGerarchieVecchie();
	caricaGerarchieNuove();
	
	
}



public static void main(String args[])
{
	char charSepArrayEquals[] = { '='};
	char charSepArraySpace[] = { ' '};
	String ar[];

	if(args.length < 1)
    {
        System.out.println("Uso: InserisciLegamiTitoloSoggettoJdbc  <file di configurazione>"); 
        System.exit(1);
    }
//    String configFile = args[0];
    String inputFile = args[0];
//	logFileOut = args[2];
    
    String start=
       "Inserisci Legami Titolo Soggetto tool - � Almaviva S.p.A 18/01/2017"+
	 "\n=================================================";


//	System.out.println("inp: " + inputFile);

	InserisciLegamiTitoloSoggettoJdbc InserisciLegamiTitoloSoggettoJdbc = new InserisciLegamiTitoloSoggettoJdbc();

//    // abbiamo parametri in ingresso?
//    for (int i=1; i < args.length; i++)
//    {
//    	ar = InserisciLegamiTitoloSoggettoJdbc.getPropertyKeyValue(args[i]);  
//    	// Add key/value pairs to the map
//    	InserisciLegamiTitoloSoggettoJdbc.parameters.put(ar[0], ar[1]);
//    }
	

	
	BufferedReader in;
	String s;
	
	try {
		in = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile),"UTF8"));

		// Leggiamo le configurazioni di base
		// ----------------------------------
		while (true) {
			try {
				s = in.readLine();
				if (s == null)
					break;
				else if (Misc.emptyString(s))
					continue;
				else if ((	s.length() == 0) 
							||  (s.charAt(0) == '#') 
							|| (Misc.emptyString(s) == true))
						continue;
				else {
					ar = InserisciLegamiTitoloSoggettoJdbc.getPropertyKeyValue(s); //MiscString.estraiCampi(s,  charSepArrayEquals, MiscStringTokenizer.RETURN_EMPTY_TOKENS_FALSE); // "="
					
					if (ar[0].startsWith("jdbcDriver"))
						InserisciLegamiTitoloSoggettoJdbc.jdbcDriver = ar[1];
					else if (ar[0].startsWith("connectionUrl"))
						InserisciLegamiTitoloSoggettoJdbc.connectionUrl = ar[1];
					else if (ar[0].startsWith("userName"))
						InserisciLegamiTitoloSoggettoJdbc.userName = ar[1];
					else if (ar[0].startsWith("userPassword"))
						InserisciLegamiTitoloSoggettoJdbc.userPassword = ar[1];
					
					else if (ar[0].startsWith("logFileOut"))
						InserisciLegamiTitoloSoggettoJdbc.logFileOut = ar[1];
					
					else if (ar[0].startsWith("tr_tit_sog_rel"))
						InserisciLegamiTitoloSoggettoJdbc.tr_tit_sog_rel = ar[1];
					
					else if (ar[0].startsWith("tb_gerarchia_soggetti"))
						InserisciLegamiTitoloSoggettoJdbc.tb_gerarchia_soggetti = ar[1];
					
					else if (ar[0].startsWith("poloStr"))
						InserisciLegamiTitoloSoggettoJdbc.poloStr = ar[1];
					else if (ar[0].startsWith("bibliotecaStr"))
						InserisciLegamiTitoloSoggettoJdbc.bibliotecaStr = ar[1];

					else if (ar[0].startsWith("priorita_polo_soggettatore"))
						InserisciLegamiTitoloSoggettoJdbc.priorita_polo_operante = Integer.parseInt(ar[1]);
					
					else if (ar[0].startsWith("progress"))
						InserisciLegamiTitoloSoggettoJdbc.progress = Integer.parseInt(ar[1], 16);

					else if (ar[0].startsWith("iniziaElaborazioneDaRiga"))
						InserisciLegamiTitoloSoggettoJdbc.iniziaElaborazioneDaRiga = Integer.parseInt(ar[1], 10);
					
					else if (ar[0].startsWith("elaboraNRighe"))
						InserisciLegamiTitoloSoggettoJdbc.elaboraNRighe = Integer.parseInt(ar[1], 10);

					else if (ar[0].startsWith("commitOgniTotRighe"))
						InserisciLegamiTitoloSoggettoJdbc.commitOgniTotRighe = Integer.parseInt(ar[1], 10);
										
					
					
					else if (ar[0].startsWith("endConfig"))
						break;

					else
						System.out.println("ERRORE: parametro sconosciuto"	+ ar[0]);
					
						
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// Apriamo il file di log
		try {

//			System.out.println("File di log: "	+ InserisciLegamiTitoloSoggettoJdbc.logFileOut);
//			InserisciLegamiTitoloSoggettoJdbc.OutLog = new BufferedWriter(new FileWriter(InserisciLegamiTitoloSoggettoJdbc.logFileOut));				
//			InserisciLegamiTitoloSoggettoJdbc.OutLog.write(start);
			
		} catch (Exception fnfEx) {
			fnfEx.printStackTrace();
			return;
		}

		// Carichiamo le gerarchie di soggettazione dei poli
		InserisciLegamiTitoloSoggettoJdbc.caricaGerarchie();
//System.exit(1);		
		
		
		
		// Apriamo il DB
		if (!InserisciLegamiTitoloSoggettoJdbc.openConnection())
			{
//			try {
//				InserisciLegamiTitoloSoggettoJdbc.OutLog.write("Failed to open DB of URL: " + InserisciLegamiTitoloSoggettoJdbc.connectionUrl);
//				InserisciLegamiTitoloSoggettoJdbc.OutLog.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			return;
			}

		
		// ----------------------------------------
//			InserisciLegamiTitoloSoggettoJdbc.fileCounter = 0;

			try {

				long startTime = System.currentTimeMillis();
				
				// Esegui trattamento delle singole tabelle
				InserisciLegamiTitoloSoggettoJdbc.doInserisciLegami();

				
				long elapsedTimeMillis = System.currentTimeMillis()-startTime;
			    s = "\n----------------------------------\nExport eseguito in " + elapsedTimeMillis/(60*1000F) + " minuti";
	
				
				System.out.println(s);
//				InserisciLegamiTitoloSoggettoJdbc.OutLog.write(s);
				
				System.out.println("Vedi " + InserisciLegamiTitoloSoggettoJdbc.logFileOut + " per i dettagli dell'elaborazione");
				// Close log file 
//				if (InserisciLegamiTitoloSoggettoJdbc.OutLog != null)
//					InserisciLegamiTitoloSoggettoJdbc.OutLog.close();
				// Close filelist input file	
				in.close();

				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// Chiudiamo il DB
			InserisciLegamiTitoloSoggettoJdbc.closeConnection();
		
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();

	} catch (UnsupportedEncodingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
//    System.out.println("Righe elaborate: " + Integer.toString(rowCounter));

	System.exit(0);
} // End main


//private void inserisciLegame(Statement stmt, String ute_ins_polo_operante, String ute_var_polo_operante, String bid, String cid)
//private void inserisciLegame(Statement stmt, String ute_ins_polo_operante, String ute_var_polo_operante, String ute_var_DB, String bid, String cid)
private void inserisciLegame(Statement stmt, String ute_ins_polo_operante,
		// int priorita_polo_operante, int priorita_polo_db,
		String ute_var_polo_operante, String ute_var_DB, String bid, String cid)
{
	
//System.out.println("cid="+cid);
	
	
	try
	{
	String ute_var_POLO = ute_var_polo_operante.substring(0, 6);  
	
//	if (!ute_ins_polo_operante.equals(ute_var_polo_operante)) // 05/04/2018 
//	if (!ute_var_POLO.equals(ute_var_DB)) // 06/04/2018 

	// if (priorita_polo_operante >= priorita_polo_db &&  !ute_var_POLO.equals(ute_var_DB)) // 06/04/2018
	// { // Se priotita' >= del polo_bib operante rispetto al polo_bib sul db e

	  // polo operante diverso da quello su DB cancella tutti i legami e reinserisci i propri
		if (! ute_var_polo_operante.substring(0,6).equals(ute_var_DB)) // 26/02/21
		{
		// CANCELLIAMO LOGICAMENTE TUTTI I LEGAMI TITOLO-SOGGETTI
		// ------------------------------------------------------
		String updateStr = "UPDATE Tr_tit_sog SET fl_canc = 'S', ute_var='"+ute_var_polo_operante+"', ts_var = SYSTIMESTAMP WHERE bid='"+bid+"'";
		stmt.execute(updateStr);
		int rowsAffected = stmt.getUpdateCount();
		cancellaTuttiILegami++;
        System.out.println("Cancello tutti i legami ("+rowsAffected+") titolo soggetto per polo operante: "+ute_var_polo_operante.substring(0, 6)+" diverso da polo DB: "+ute_var_DB);
		}
		//System.out.println("Legami cancellati logicamente="+rowsAffected);
	// }
		
	
	
	// Proviamo ad INSERIRE il legame	
	String insertStr = "insert into tr_tit_sog  values('"+bid+"','"+cid+"','"+ute_ins_polo_operante+"',SYSTIMESTAMP,'"+ute_var_polo_operante+"',SYSTIMESTAMP,'N')";

//	boolean retB =  stmt.execute(insertStr);
//	if (retB == true)
//	{
//		System.out.println("Inserito legame: "+bid+"->"+cid);
//		legamiInseriti++;
//	}
//	else
//	{
//		System.out.println("Inserito legame retB=FALSE: "+bid+"->"+cid);
//		legamiInseriti++;
//		
//	}
	
	stmt.execute(insertStr);
	System.out.println("Inserito legame: "+bid+"->"+cid);
	legamiInseriti++;
	
	
//stmt.execute("COMMIT");									
	}
	catch (Exception e) {
		// Legame cancellato logicamente.
		// RIPRISTINO IL LEGAME
		String updateStr = "UPDATE Tr_tit_sog SET fl_canc = 'N', ute_var='"+ute_var_polo_operante+"', ts_var = SYSTIMESTAMP WHERE bid='"+bid+"' and cid='"+cid+"'" ;
		try {
			stmt.execute(updateStr);
			System.out.println("Inserito legame per ripristino: "+bid+"->"+cid);
			legamiInseriti++;
			
		} catch (SQLException e1) {
			System.out.println("Errore in inserimento legame per ripristino: "+bid+"->"+cid);
			legamiNonInseriti++;
		}
//stmt.execute("COMMIT");										
	}
}

	
@SuppressWarnings("unused")
public void doInserisciLegami() 
{
//	String arTable[];
	
	
//	String tableName = "tr_tit_sog";
	Statement stmt = null;
	//int rows=0;
	String legami;
//	String fileIdDaRimuovere="";
	
//	int idTabella = -1;
	
	// Apriamo il file dei legami titolo soggetto
	try {

		// Apri file degli identificativi in lettura
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(tr_tit_sog_rel),"UTF8"));
		stmt = con.createStatement();
		
		// Elabora
		int recordCancellati=0;
		int rowCtr=0;
		int righeElaborate = 0;

		while (true) {
			try {
//				// Commit every tot records
//				if ((rowCtr & 0x3FF) == 0) // Mettiamo ogni 1024 records
//				// if ((rowCtr & 0xF) == 0) // Mettiamo ogni 1024
//				// records
//				{
//					System.out.print("\rCommitting at row " + rowCtr);
//					stmt.execute("COMMIT"); // ;
//					sqlExecutedOk.clear();
//					rollbackRecordsFrom = rowCtr;
//				}
				
				
				legami = in.readLine();
				rowCtr++;



				if (rowCtr < iniziaElaborazioneDaRiga)
					continue;
				
				
				
				if (legami == null)
					break;
				else {
					if ((	legami.length() == 0) 
							||  (legami.charAt(0) == '#') 
							|| (Misc.emptyString(legami) == true))
						continue;
					

				String legamiAR[] = legami.split("\\|");
				
				String cid = legamiAR[0];
				String bid, cdPoloOperante, cdBiblioteca;
				String cdPoloBibliotecaStr;
				for (int i=1; i < legamiAR.length; i++)
				{
					titoli_da_legare++;
					 System.out.println("Da legare: "+cid+"->"+legamiAR[i]);

					
					
					String bidPoloBibAR[] = legamiAR[i].split(",");
					bid = bidPoloBibAR[0];
//					cdPoloOperante = bidPoloBibAR[1];
//					cdBiblioteca = bidPoloBibAR[2];
					cdPoloBibliotecaStr = bidPoloBibAR[1]+bidPoloBibAR[2]; // cdPoloOperante+cdBiblioteca;
					String cdPoloAltreBibStr = bidPoloBibAR[1]+" **"; // cdPoloOperante+cdBiblioteca;
					
					String ute_ins_polo_operante = cdPoloBibliotecaStr+"import"; // cdPoloOperante+cdBiblioteca+"import";
					String ute_var_polo_operante = ute_ins_polo_operante; 

					// Polo operante
					Integer prioritaPoloOp = prioritaMap.get(cdPoloBibliotecaStr); // cdPoloOperante 

					// if ( prioritaPoloOp == null)
						// prioritaPoloOp = prioritaMap.get(cdPoloAltreBibStr);
					
					// if ( prioritaPoloOp == null)
					// {
						// System.out.println(bid+"->"+cid+" - Polo/Biblioteca operante ("+cdPoloBibliotecaStr+") non dichiarato nella gerarchia");
						// continue;
					// }
					
					// 08/10/2019 Controlliamo che il titolo non sia stato cancellato
					ResultSet rs = stmt.executeQuery("select bid from tb_titolo where bid='"+bid+"' and fl_canc != 'S'" );
					if(!rs.next())
					{
						System.out.println("Errore in inserimento legame: "+bid+"->"+cid+" non esiste nella tabella dei titoli");
						legamiNonInseriti++;
						continue;
					}
					
					
					
					// Facciamo i controlli di priorita'
					// Troviamo tutti i legami a soggetti del titolo
					rs = stmt.executeQuery("select ute_var from tr_tit_sog where bid='"+bid+"' and fl_canc != 'S'" );
					if(rs.next())
					{
//						String ute_var_DB = rs.getString(1).substring(0, 6);
//						if (!ute_var_DB.equals(cdPoloBibliotecaStr))
						
//						String polo_DB = rs.getString(1).substring(0, 3); // 15/06/2017 gestione polo biblioteca ancora non attiva in indice
						String ute_var_DB = rs.getString(1).substring(0, 6); // 15/06/2017 gestione polo biblioteca ancora non attiva in indice

						Integer prioritaPoloDb = prioritaMap.get(ute_var_DB); // Polo db e' soggettatore? 

// System.out.println("prioritaPoloDb="+prioritaPoloDb);

// System.out.println("ute_var_DB="+ute_var_DB);

// , prioritaPoloDb="+prioritaPoloDb);
						
						if ( prioritaPoloDb == null)
						{
// System.out.println("ute_var_DB="+ute_var_DB + "Check **");
// System.out.println("prioritaPoloDb="+prioritaPoloDb);

							prioritaPoloDb = prioritaMap.get(ute_var_DB.substring(0, 3)+" **");
							
// System.out.println("prioritaPoloDb="+prioritaPoloDb);
							
						}

						if (prioritaPoloOp != null && prioritaPoloDb != null)
						{

							// if (prioritaPoloDb != null)
							// {
								// { 19/04/2018 cerchiamo la priorita' + alta
								String ute_var_DB_max = ute_var_DB;
								Integer prioritaPoloDbMax = prioritaPoloDb;	
								while (rs.next())
								{
									// ute_var_DB = rs.getString(1).substring(0, 6); // 15/06/2017 gestione polo biblioteca ancora non attiva in indice
									// prioritaPoloDb = prioritaMap.get(ute_var_DB); // Polo db e' soggettatore?
									// if ( prioritaPoloDb == null)
										// prioritaPoloDb = prioritaMap.get(ute_var_DB.substring(0, 3)+" **");
									
									// if (prioritaPoloDb != null )
									// {
										if (prioritaPoloDbMax != null && prioritaPoloDb > prioritaPoloDbMax)
										{
											ute_var_DB_max = ute_var_DB;
											prioritaPoloDbMax = prioritaPoloDb;	
										}
										else
										{
											ute_var_DB_max = ute_var_DB;
											prioritaPoloDbMax = prioritaPoloDb;	
										}
										
									// }
									
								}
								ute_var_DB = ute_var_DB_max;
								prioritaPoloDb = prioritaPoloDbMax;	

// System.out.println(bid+"->"+cid+" - GERARCHIA + ALTA=prioritaPoloDb " + prioritaPoloDb);

							// }
						}
						// }
						
						
						if ( prioritaPoloOp != null)
						{ // Polo operante SOGGETTATORE
							// controlla chi ha priorita' + alta
							// se priorita polo op < priorita' polo DB non si pio' fare niente
							
							// Se i due polo sono soggettatori	
							if (prioritaPoloDb == null)
							{ // Polo operante SOGGETTATORE e polo db NON soggettatore    
								if (!ute_var_DB.equals("XXX"))
								{ // Inserisci
									prioritaPoloDb=0; // non soggettatore	
									// inserisciLegame(stmt, ute_ins_polo_operante, prioritaPoloOp, prioritaPoloDb, ute_var_polo_operante, ute_var_DB, bid, cid);
									inserisciLegame(stmt, ute_ins_polo_operante, ute_var_polo_operante, ute_var_DB, bid, cid);									
// System.out.println(bid+"->"+cid+" - INSERISCI LEGAME");

									}
								else
								{
//									System.out.println(bid+"->"+cid+" - @@@@ Priorita' del polo/bib operante ("+cdPoloBibliotecaStr+") e' inferiore al polo/bib sul DB ("+ute_var_DB+")");
									System.out.println("Errore in inserimento legame: "+bid+"->"+cid+" - @@@@ Priorita' del polo/bib operante ("+cdPoloBibliotecaStr+") e' inferiore al polo/bib sul DB ("+ute_var_DB+")");
									legamiNonInseriti++;
								}
							}
							else
							{
								// I due poli sono soggettatori 
								if (prioritaPoloOp < prioritaPoloDb)
									{ // priorita polo operante < priorita polo db   
										System.out.println("Errore in inserimento legame: "+bid+"->"+cid+" -##### Priorita' del polo/bib operante ("+cdPoloBibliotecaStr+") e' inferiore al polo/bib sul DB ("+ute_var_DB+")");
										legamiNonInseriti++;
									}
								else
									{ // priorita polo operante >= priorita polo db
									// inserisciLegame(stmt, ute_ins_polo_operante, prioritaPoloOp, prioritaPoloDb, ute_var_polo_operante, ute_var_DB, bid, cid);
									inserisciLegame(stmt, ute_ins_polo_operante, ute_var_polo_operante, ute_var_DB, bid, cid);									
									}
								}
							}
						else
							{ // Polo operante NON e' SOGGETTATORE

							// se ute var db e' soggettatore non si puo' modificare
							if (prioritaPoloDb != null)
							{ // Polo operante NON SOGGETTATORE e polo db SOGGETTATORE    
									System.out.println("Errore in inserimento legame: "+bid+"->"+cid+" -$$$$  Priorita' del polo/bib operante ("+cdPoloBibliotecaStr+") e' inferiore al polo/bib sul DB ("+ute_var_DB+")");
									legamiNonInseriti++;

							}
							else
							{	// POLO/BIB OPERANTE E POLO/BIB DB NON SOGGETTATORI
								// ute var db  non e' soggettatore. Se sono diversi non si puo modificare 
//								if (!cdPoloOperante.equals(polo_DB))
								if (!cdPoloBibliotecaStr.equals(ute_var_DB))
								{ // Polo operante NON uguale a polo in DB    
									System.out.println("Errore in inserimento legame: "+bid+"->"+cid+" polo/bib operante ("+cdPoloBibliotecaStr+") non soggettatore e diverso da  polo/bib sul DB ("+ute_var_DB+")");
									legamiNonInseriti++;
								}
								else
								{
									prioritaPoloDb=0; // non soggettatore
									// inserisciLegame(stmt, ute_ins_polo_operante, prioritaPoloOp, prioritaPoloDb, ute_var_polo_operante, ute_var_DB, bid, cid);
									inserisciLegame(stmt, ute_ins_polo_operante, ute_var_polo_operante, ute_var_DB, bid, cid);
								}
								
							}
						}
						
					}
					else
					{ // Primo Legame 
						try
						{
							// INSERIAMO il primo legame	
							String insertStr = "insert into tr_tit_sog  values('"+bid+"','"+cid+"','"+ute_ins_polo_operante+"',SYSTIMESTAMP,'"+ute_var_polo_operante+"',SYSTIMESTAMP,'N')";
	
//							boolean retB =  stmt.execute(insertStr);
//							if (retB == true)
//							{
//								System.out.println("Inserito il primo legame: " +bid+"->"+cid);
//								legamiInseriti++;
//							}
//							else
//							{
//								System.out.println("Inserito il primo legame retB=FALSE: " +bid+"->"+cid);
//								legamiInseriti++;
//							}
							stmt.execute(insertStr);
							System.out.println("Inserito il primo legame " +bid+"->"+cid);
							legamiInseriti++;
								
						}
						catch (Exception e) {
							// Legame cancellato logicamente.
							// Ripristino il legame
							String updateStr = "UPDATE Tr_tit_sog SET fl_canc = 'N', ute_var='"+ute_var_polo_operante+"', ts_var = SYSTIMESTAMP WHERE bid='"+bid+"' and cid='"+cid+"'";
							try {
//								boolean retB =  stmt.execute(updateStr);
								stmt.execute(updateStr);
								System.out.println("Inserito il primo legame per update: " +bid+"->"+cid);
								legamiInseriti++;
								
							} catch (SQLException e1) {
								System.out.println("Errore in inserimento " +bid+"->"+cid+" RIPRISTINO primo legame fallito");
								legamiNonInseriti++;
							}
						}
						
//stmt.execute("COMMIT");
						
					}
				
				}
				
				righeElaborate++;
				if ((righeElaborate & progress) == progress)
					System.out.println("Righe elaborate: " + righeElaborate + " ");
				
				
				if ((righeElaborate % commitOgniTotRighe) == 0)
				{
					stmt.execute("COMMIT");
					System.out.println("Committato a " + righeElaborate + " righe elaborate");
				}
				
				
				if (elaboraNRighe != 0 && righeElaborate >= elaboraNRighe)	
					break;

				
				} // End riga con testo
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} // End while

stmt.execute("COMMIT"); 
System.out.println("Committato a " + righeElaborate + " Righe elaborate");
		
		System.out.println("Righe lette: " + rowCtr );
		System.out.println("Righe righeElaborate: " + righeElaborate);
		System.out.println("Titoli da legare: " +titoli_da_legare );
//		System.out.println("Legami aggiornati: " +legamiAggiornati );
		System.out.println("Legami inseriti: " +legamiInseriti );
		System.out.println("Legami NON inseriti: " +legamiNonInseriti );
		System.out.println("Volte che sono stati cancellati tutti i legami: " + cancellaTuttiILegami);
		
//		OutLog.write("\n"+s);
		
		try {
			in.close();
			stmt.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	} catch (Exception fnfEx) {
		fnfEx.printStackTrace();
		return;
	}
} // End doRimuovi


} // End 	
