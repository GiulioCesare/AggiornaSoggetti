/**

Batch per inserimento legami titolo soggetto

/media/export54/indice/dp/cfi_soggetti/FondiSoggettiControllatoJdbc.txt

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

import it.finsiel.misc.Misc;


public class FondiSoggettiControllatoJdbc {

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
	String logFileOut = "fondiSoggettiControllatoJdbc.log";
	BufferedWriter OutLog;
	String fileOut = "";
	boolean setCurCfg=false;
	//FileOutputStream streamOutLog;
	
	BufferedOutputStream bufferedStreamOutTable;	// 15/09/2010
	BufferedOutputStream bufferedStreamOutTableBytes;

	
	Connection con = null;
	int commitOgniTotRighe = 10;
	String jdbcDriver="";
	String connectionUrl = ""; // "jdbc:postgresql://localhost:5432/sbnwebArge"
	String userName="";
	String userPassword="";
	int progress=0x3ff; 
	String listaSoggettiDaFondere;
	//String tb_gerarchia_soggetti;
	String poloStr;
	String bibliotecaStr;
	int priorita_polo_operante;
	int iniziaElaborazioneDaRiga;
	int elaboraNRighe;
	
	
	String fileIdDaRimuovere = null;
	String query;
	String preprocess;

	int legamiInseriti = 0;
//	int legamiNonInseriti = 0;
	int legamiAggiornati = 0;
	
	public FondiSoggettiControllatoJdbc()
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
		String s = "Conncecting to " + connectionUrl + " userName=" + userName;
		try {
			//System.out.println(s);
			OutLog.write(s+"\n");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
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

//private void caricaGerarchie()
//{
//	BufferedReader in;
//	String s;
//
//	try {
//		in = new BufferedReader(new InputStreamReader(new FileInputStream(tb_gerarchia_soggetti),"UTF8"));
//		String key;
//		int value;
//		// Leggiamo le configurazioni di base
//		// ----------------------------------
//		while (true) {
//			try {
//				s = in.readLine();
//				if (s == null)
//					break;
//				else if (Misc.emptyString(s))
//					continue;
//				else if ((	s.length() == 0) 
//							||  (s.charAt(0) == '#') 
//							|| (Misc.emptyString(s) == true))
//						continue;
//
//				key = s.substring(0,6); 
//				value = Integer.parseInt(s.substring(6,8));
//
//				prioritaMap.put(key, value);
//			
//			
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				}
//			}
//		}
//	 catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//		System.exit(1);
//	}
//	
//}



public static void main(String args[])
{
	char charSepArrayEquals[] = { '='};
	char charSepArraySpace[] = { ' '};
	String ar[];

	if(args.length < 1)
    {
        System.out.println("Uso: FondiSoggettiControllatoJdbc  <file di configurazione>"); 
        System.exit(1);
    }
//    String configFile = args[0];
    String inputFile = args[0];
//	logFileOut = args[2];
    
    String start=
       "Fondi Soggetti - � Almaviva S.p.A 04/05/2017"+
	 "\n=================================================";


//	System.out.println("inp: " + inputFile);

	FondiSoggettiControllatoJdbc fondiSoggettiControllatoJdbc = new FondiSoggettiControllatoJdbc();


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
					ar = fondiSoggettiControllatoJdbc.getPropertyKeyValue(s); //MiscString.estraiCampi(s,  charSepArrayEquals, MiscStringTokenizer.RETURN_EMPTY_TOKENS_FALSE); // "="
					
					if (ar[0].startsWith("jdbcDriver"))
						fondiSoggettiControllatoJdbc.jdbcDriver = ar[1];
					else if (ar[0].startsWith("connectionUrl"))
						fondiSoggettiControllatoJdbc.connectionUrl = ar[1];
					else if (ar[0].startsWith("userName"))
						fondiSoggettiControllatoJdbc.userName = ar[1];
					else if (ar[0].startsWith("userPassword"))
						fondiSoggettiControllatoJdbc.userPassword = ar[1];
					
					else if (ar[0].startsWith("logFileOut"))
						fondiSoggettiControllatoJdbc.logFileOut = ar[1];
					
					else if (ar[0].startsWith("listaSoggettiDaFondere"))
						fondiSoggettiControllatoJdbc.listaSoggettiDaFondere = ar[1];
					
//else if (ar[0].startsWith("tb_gerarchia_soggetti"))
//	FondiSoggettiControllatoJdbc.tb_gerarchia_soggetti = ar[1];
					
					else if (ar[0].startsWith("poloStr"))
						fondiSoggettiControllatoJdbc.poloStr = ar[1];
					else if (ar[0].startsWith("bibliotecaStr"))
						fondiSoggettiControllatoJdbc.bibliotecaStr = ar[1];

					else if (ar[0].startsWith("priorita_polo_operante"))
						fondiSoggettiControllatoJdbc.priorita_polo_operante = Integer.parseInt(ar[1]);
					
					else if (ar[0].startsWith("progress"))
						fondiSoggettiControllatoJdbc.progress = Integer.parseInt(ar[1], 16);

					else if (ar[0].startsWith("iniziaElaborazioneDaRiga"))
						fondiSoggettiControllatoJdbc.iniziaElaborazioneDaRiga = Integer.parseInt(ar[1], 10);
					
					else if (ar[0].startsWith("elaboraNRighe"))
						fondiSoggettiControllatoJdbc.elaboraNRighe = Integer.parseInt(ar[1], 10);

					else if (ar[0].startsWith("commitOgniTotRighe"))
						fondiSoggettiControllatoJdbc.commitOgniTotRighe = Integer.parseInt(ar[1], 10);
										
					
					
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

			System.out.println("File di log: "	+ fondiSoggettiControllatoJdbc.logFileOut);
			fondiSoggettiControllatoJdbc.OutLog = new BufferedWriter(new FileWriter(fondiSoggettiControllatoJdbc.logFileOut));				
			fondiSoggettiControllatoJdbc.OutLog.write(start+"\n");
			
		} catch (Exception fnfEx) {
			fnfEx.printStackTrace();
			return;
		}

		// Carichiamo le gerarchie di soggettazione dei poli
//		FondiSoggettiControllatoJdbc.caricaGerarchie();
//System.exit(1);		
		
		
		
		// Apriamo il DB
		if (!fondiSoggettiControllatoJdbc.openConnection())
			{
//			try {
//				FondiSoggettiControllatoJdbc.OutLog.write("Failed to open DB of URL: " + FondiSoggettiControllatoJdbc.connectionUrl);
//				FondiSoggettiControllatoJdbc.OutLog.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			return;
			}

		
		// ----------------------------------------
//			FondiSoggettiControllatoJdbc.fileCounter = 0;

			try {

				long startTime = System.currentTimeMillis();
				
				// Esegui trattamento delle singole tabelle
				fondiSoggettiControllatoJdbc.doFondiSoggettiControllato();

				
				long elapsedTimeMillis = System.currentTimeMillis()-startTime;
			    s = "----------------------------------\nFondiSoggettiControllatoJdbc eseguito in " + elapsedTimeMillis/(60*1000F) + " minuti";
//				System.out.println(s);
				fondiSoggettiControllatoJdbc.OutLog.write(s+"\n");

				System.out.println("Vedi " + fondiSoggettiControllatoJdbc.logFileOut + " per i dettagli dell'elaborazione");

				if (fondiSoggettiControllatoJdbc.OutLog != null)
					fondiSoggettiControllatoJdbc.OutLog.close();
				// Close filelist input file	
				in.close();

				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// Chiudiamo il DB
			fondiSoggettiControllatoJdbc.closeConnection();
		
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

public void doFondiSoggettiControllato() 
{
//	String arTable[];
	
	
//	String tableName = "tr_tit_sog";
	Statement stmt = null;
	//int rows=0;
	String riga;
//	String fileIdDaRimuovere="";
	
//	int idTabella = -1;
	
	// Apriamo il file dei legami titolo soggetto
	try {

		// Apri file degli identificativi in lettura
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(listaSoggettiDaFondere),"UTF8"));
		stmt = con.createStatement();
		Statement stmt1 = con.createStatement();

		
		// Elabora
		int recordCancellati=0;
		int rowCtr=0;
		int righeElaborate = 0;
		int legamiTitoloSpostatiCtr=0;
		int descrittoriSoggettoRimossiCtr=0;
		int cidVarRimossiCtr=0;
		int soggettiRimossiCtr=0;
		String updateStr, deleteStr;

		int soggettiFusiCtr=0;
		
		while (true) {
			try {
								
				riga = in.readLine();
				rowCtr++;
				if (rowCtr < iniziaElaborazioneDaRiga)
					continue;
				
				if (riga == null)
					break;
				if ((	riga.length() == 0) 
						||  (riga.charAt(0) == '#') 
						|| (Misc.emptyString(riga) == true))
					continue;
					
				righeElaborate++;
				
				String fondiAR[] = riga.split("\\|");
				
//				String cid_from = fondiAR[0];	// CID_ACC
//				String cid_to = fondiAR[1];		// CID_VAR
				// 16/06/17
				String cid_from = fondiAR[1];	// CID_VAR
				String cid_to = fondiAR[0];		// CID_ACC
				
				String cd_soggettario_polo = fondiAR[2];
				
				String cid_sopravissuto = cid_to;

				int cd_livello_from=0, cd_livello_to_in_db=0 ,cd_livello_to=0;
				String cd_sog_from_db, cd_sog_to_db; 
				
//System.out.println();
OutLog.write("Fusione di "+cid_from+" su "+cid_to+"\n");



// Prendiamo i due soggetti in questione e controlliamo che esistano entrambi
query = 	"select * from tb_soggetto where cid in ('"+cid_from+"','"+cid_to+"') and fl_canc != 'S'";
//System.out.println("-------------------------------------"+);
//System.out.println(query);
ResultSet rs = stmt.executeQuery(query);

while (rs.next())
{
	String cid = rs.getString("cid");
	if (cid.equals(cid_from))
	{
		cd_livello_from = Integer.parseInt(rs.getString("cd_livello"));
		cd_sog_from_db =  rs.getString("cd_soggettario");
	}
	else
	{
		cd_livello_to_in_db = Integer.parseInt(rs.getString("cd_livello"));
		cd_livello_to = priorita_polo_operante;
		cd_sog_to_db =  rs.getString("cd_soggettario");
	}
}				
if (cd_livello_from == 0 || cd_livello_to == 0)
{
	if (cd_livello_from == 0)
		OutLog.write("FUSIONE NON AMMESSA: Soggetto " +cid_from+" non presente o cancellato"+"\n");
	else
		OutLog.write("FUSIONE NON AMMESSA: Soggetto " +cid_to+" non presente o cancellato"+"\n");
	continue;
}

//System.out.println("cid_from "+cid_from+", livello from="+cd_livello_from+", cid_to "+cid_to+", livello polo="+cd_livello_to+", livello to in db="+cd_livello_to_in_db);



// Verichiamo chi fonde su chi!!!
// Se codice livello from > codice livello to di arrivo il cid_di arrivo diventa cid di partenza
// NON PER ORA. Operazone non ammessa. Segnala

// 16/06/2017 Dopo chiacchierata telefonica con Rossana 
//if (cd_livello_to < cd_livello_from)
//{	
//	// Scambia partenza ed arrivo legame
////	System.out.println("Codice livello del polo operante ("+cd_livello_to+") < del codice livello del polo di partenza"+cd_livello_from+" scambio cid_from "+cid_from+" con cid_to"+cid_to);
////	String cid_tmp = cid_from;
////	cid_to = cid_from;
////	cid_from = cid_tmp;
//
//	System.out.println("FUSIONE NON AMMESSA: codice di livello del soggetto di arrivo non sufficiente: "+cd_livello_to+"  cd livello partenza"+cd_livello_from + " per cid_from "+cid_from + " to cid_to "+cid_to);
//continue;
//}


				// Spostiamo i legami a titoli da cid_from a cid_to
				//String query = "select * from MULTIMATERIALE.tr_tit_sog where cid='"+cid_from+"'";
					query = "select bid from tr_tit_sog where cid='"+cid_from+"' and fl_canc != 'S'";
//					System.out.println(query);
					rs = stmt.executeQuery(query);
					// Ci sono dei legami da aggiornare?
					
					// Per evitare di aggiornare e generare legami gia' esistenti, cancellati o no lavoriamo sui singoli legami
					while (rs.next())
					{
						String bid = rs.getString("bid");

						try {
							updateStr = "UPDATE tr_tit_sog SET cid = '"+cid_to+"', ute_var='"+poloStr+bibliotecaStr+"import', ts_var = SYSTIMESTAMP WHERE bid='"+bid+"' and cid='"+cid_from+"'";
//							System.out.println(updateStr);
							legamiTitoloSpostatiCtr +=  stmt1.executeUpdate(updateStr);
						}
						catch (SQLException e)
						{
							// e.printStackTrace();
							// Chiave gia esistente?
							if (e.getErrorCode() == 1)
							{ // SI
								// aggiorna fl_canc a N
								updateStr = "UPDATE tr_tit_sog SET fl_canc = 'N', ute_var='"+poloStr+bibliotecaStr+"import', ts_var = SYSTIMESTAMP WHERE bid='"+bid+"' and cid='"+cid_to+"'";
//								System.out.println(updateStr);
								legamiTitoloSpostatiCtr +=  stmt1.executeUpdate(updateStr);

								// Cancelliamo logicamente il record con cid_from
								updateStr = "UPDATE tr_tit_sog SET fl_canc = 'S', ute_var='"+poloStr+bibliotecaStr+"import', ts_var = SYSTIMESTAMP WHERE bid='"+bid+"' and cid='"+cid_from+"'";
//								System.out.println(updateStr);
								stmt1.executeUpdate(updateStr);
							}
						}
					} // end while 
				
				// Cancelliamo i legami a descrittori da cid_from a cid_to
				deleteStr = "UPDATE tr_sog_des set fl_canc = 'S', ute_var='"+poloStr+bibliotecaStr+"import', ts_var = SYSTIMESTAMP where cid='"+cid_from+"'";
//				System.out.println(deleteStr);
				descrittoriSoggettoRimossiCtr +=  stmt.executeUpdate(deleteStr);
				
				
				// Se cid_sopravissuto e' il cid_var nella ts_cid_var
				// RIMUOVERE legame cid_var -> cid acc in ts_cid_var.
				// E.g.: A viene fuso su B. A sparisce nei soggetti ed anche in TS_CID_VAR 
				//	CID_ACC		CID_VAR
				//	A*		<=	B
				//	B
				rs = stmt.executeQuery("select cid_var from ts_cid_var where cid_var='"+cid_sopravissuto+"' and fl_canc != 'S'" );
				if(rs.next())
				{
					deleteStr = "UPDATE ts_cid_var set fl_canc = 'S', ute_var='"+poloStr+bibliotecaStr+"import', ts_var = SYSTIMESTAMP where cid_var='"+cid_sopravissuto+"'";
//					System.out.println(deleteStr);
					cidVarRimossiCtr +=  stmt.executeUpdate(deleteStr);
				}
				
				// Se cid_sopravissuto e' il cid_acc nella ts_cid_var
				// la ts_cid_var rimane INVARIATA
				// E.g.: B viene fuso su A. B viene rimosso nei soggetti e rimane in TS_CID_VAR 
				//	CID_ACC		CID_VAR
				//	A		<=	B
				//	B*
				
				// Rimuovere il soggetto fuso (cid_from)! 
				deleteStr = "UPDATE tb_soggetto set fl_canc = 'S', ute_var='"+poloStr+bibliotecaStr+"import', ts_var = SYSTIMESTAMP where cid='"+cid_from+"'";
//				System.out.println(deleteStr);
				soggettiRimossiCtr +=  stmt.executeUpdate(deleteStr);
				
				// Aggiorniamo il soggetto sopravissuto con 
				//	- il codice soggettario di polo 
				//	- il codice livello di indice
				updateStr = "UPDATE tb_soggetto SET cd_soggettario = '"+cd_soggettario_polo+"', cd_livello='"+cd_livello_to+"', ute_var='"+poloStr+bibliotecaStr+"import', ts_var = SYSTIMESTAMP WHERE cid='"+cid_to+"'";
//				System.out.println(updateStr);
				soggettiRimossiCtr +=  stmt.executeUpdate(updateStr);

				soggettiFusiCtr++;
				
				if ((soggettiFusiCtr%commitOgniTotRighe) == 0)
				{
					stmt.execute("COMMIT"); 
					OutLog.write("****Committato a " + soggettiFusiCtr + " soggetti fusi"+"\n");
				}
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} // End while

		stmt.execute("COMMIT"); 
		
		OutLog.write("Soggetti da fondere: " + righeElaborate+"\n");
		OutLog.write("Soggetti fusi: " + soggettiFusiCtr+"\n");
		
//		System.out.println("Legami Titolo-Soggetto spostati: " +  legamiTitoloSpostatiCtr);
//		System.out.println("Legami a descrittori di soggetto rimossi: " +descrittoriSoggettoRimossiCtr );
//		System.out.println("Legami a cid accettati rimossi: " +cidVarRimossiCtr );
//		System.out.println("Soggetti rimossi: " + soggettiRimossiCtr);
		
		
		
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
} // End doFondiSoggetti 


} 
