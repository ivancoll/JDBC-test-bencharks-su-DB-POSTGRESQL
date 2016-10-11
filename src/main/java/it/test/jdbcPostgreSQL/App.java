package it.test.jdbcPostgreSQL;

import java.sql.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * La classe DBOperation contiene i metodi utilizzati
 * per i benchmark di INSERT e SELECT su un DB JDBC compliant
 *  
 * <p>
 * I metodi esposti sono 
 *
 * - createTable --> per
 *   la creazione di una tabella di DB sulla quali eseguire 
 *	 i benchmarks.	
 *	 
 * - DropTable --> per droppare la tabella e ricrearla
 *	 ad ogni lancio dell'applicazione. 
 *
 * - InsertIntoTable --> per eseguire i benchmarks degli statement
 *   di INSERT con commit ogni X statement. Update sequenziali e
 *   a blocchi. 
 *
 * - SelectFromTableTime --> per eseguire i benchmarks degli statement
 *   di SELECT per PK della tabella creata in precedenza.
 *
 * - CloseConn --> per chiudere la connessione al DB 	 
 *
 */ 
class DBOperation
{
  /**
   * Questo metodo è usato per restituire la data e l'ora attuali
   *
   * @return java.sql.Date Restituisce la data/ora attuale.
   */	
   
   private static java.sql.Date getCurrentDate() {
		java.util.Date today = new java.util.Date();
		return new java.sql.Date(today.getTime());
   }    	
   
  /**
   * Questo metodo è usato per verificare l'esistenza di una
   * tabella sul DB.
   * 
   * 
   * @param conn connessione al DB
   * @param tableName  Nome della tabella
   * @return boolean Restituisce true se la tabella esiste.
   */	
   public static boolean tableExist(Connection conn, String tableName) throws SQLException {
			boolean tExists = false;
			String tName="";
			ResultSet rs=null;
			
			try {
				
				rs = conn.getMetaData().getTables(null, null, tableName, null); 
				while (rs.next()) { 
					 tName = rs.getString("TABLE_NAME");					 
					 
					 if (tName != null && tName.toUpperCase().equals(tableName.toUpperCase())) {
						tExists = true;
						break;
					}
				}
			}			
			catch (SQLException e) {

				System.out.println(e.getMessage());
				throw e;
			}
			finally {
				if (rs != null) rs.close(); 
			}	
			
			return tExists;
	}
	
  /**
   * Questo metodo è usato per creare su DB la tabella
   * sulla quale eseguire i benchmark di INSERT/SELECT 
   * 
   * @param conn  connessione al DB
   * @param asTableName  Nome della tabella
   */	
	public void createTable(Connection conn, String asTableName) throws SQLException {

		PreparedStatement preparedStatement = null;
		
		String CreateTableSQL = "CREATE TABLE IF NOT EXISTS "+asTableName+"("
				+ "ID INTEGER CONSTRAINT "+asTableName+"_pk PRIMARY KEY, "
				+ "NAME VARCHAR(255) NOT NULL, "
				+ "SURNAME VARCHAR(255) NOT NULL, "
				+ "CREATED_DATE DATE NOT NULL " 
				+ "); "
				+ "COMMENT ON TABLE "+asTableName+" IS 'Tabella di test per benchmarks JBDC';"
				+ "COMMENT ON COLUMN "+asTableName+".ID IS 'Primary Key.';"
				+ "COMMENT ON COLUMN "+asTableName+".CREATED_DATE IS 'Timestamp';";		   	

		try {
			preparedStatement = conn.prepareStatement(CreateTableSQL);

			preparedStatement.executeUpdate();

		} catch (SQLException e) {

			System.out.println(e.getMessage());
			throw e;

		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}	
	
  /**
   * Questo metodo è usato per eseguire il drop della tabella
   * sui cui vengono eseguiti i benchmark. La tabella ad ogni
   * avvio dell'applicazione viene droppata e ricreata.
   * 
   * @param conn  connessione al DB
   * @param asTableName  Nome della tabella
   */
	public void DropTable(Connection conn, String asTableName) throws SQLException {

		PreparedStatement preparedStatement = null;
		String DropTableSQL = "DROP TABLE IF EXISTS "+asTableName+";";

		try {
			preparedStatement = conn.prepareStatement(DropTableSQL);

			// System.out.println(DropTableSQL);

			// execute create SQL stetement
			preparedStatement.executeUpdate();

		} catch (SQLException e) {

			System.out.println(e.getMessage());
			throw e;

		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}	
		}
	}
	
  /**
   * Questo metodo è usato per eseguire i benchmark di inserimento
   * in tabella. I benchmark vengono calcolati per inserimenti 
   * batch a blocchi di X statement, oppure per inserimenti 
   * sequenziali. I commit vengono eseguiti ogni X statement.
   * 
   * 
   * @param conn  connessione al DB
   * @param asTableName  Nome della tabella
   * @param ai_max_rows_per_commit  Numero massimo di INSERT per ogni commit
   * @param ai_max_rows_inserted  Numero massimo di righe da inserire
   * @param abSequentialUpdate  Se (false) indica di eseguire gli insert massivi a
   *									   blocchi di X statement (esecuzione batch)
   *									   Se (true) esegue gli insert in maniera sequenziale
   *									   (uno per ogni iterazione).  
   *
   */
	public void InsertIntoTable(Connection conn, String asTableName, int ai_max_rows_per_commit, int ai_max_rows_inserted, boolean abSequentialUpdate) throws SQLException {

		PreparedStatement preparedStatement = null;
		long l_startTime=0;
		long l_elapsedTime=0;
		long l_min_elapsed_time=Long.MAX_VALUE;
		long l_max_elapsed_time=0;
		double ld_average_time=0;
		long l_tot_elapsed_time=0;
		int l_count=0;

		String InsertIntoTableSQL = " insert into "+asTableName+" (id, name, surname, created_date)"
               + " values (?, ?, ?, ?)";

		try {
			conn.setAutoCommit(false);
			preparedStatement = conn.prepareStatement(InsertIntoTableSQL);
			
			for(int i=1; i <= ai_max_rows_inserted; i++) {
				 l_count++;
				 l_startTime = System.nanoTime(); // start
				 preparedStatement.setInt(1, i);
				 preparedStatement.setString(2, "test_name" + i);
				 preparedStatement.setString(3, "test_surname" + i);
				 preparedStatement.setDate(4, getCurrentDate());
				 if (abSequentialUpdate) {
					 // esecuzione UPDATE in sequenza (uno per ogni iterazione del ciclo)
					 preparedStatement.executeUpdate(); 						
				 }
				 else {
					 // preparazione batch di update per blocchi di statement
					 preparedStatement.addBatch();
			     }				 					

				if (i % ai_max_rows_per_commit == 0 || i == ai_max_rows_inserted) {
					if (!abSequentialUpdate) {
						preparedStatement.executeBatch(); // INSERT batch ogni X insert.
					}	
            
					conn.commit(); // commit ogni ai_max_rows_per_commit statement di UPDATE
				}
				
				l_elapsedTime = System.nanoTime() - l_startTime;
				
				l_tot_elapsed_time+=l_elapsedTime;
				
				if (l_elapsedTime > 0) {
					if (l_elapsedTime > l_max_elapsed_time) {
						l_max_elapsed_time = l_elapsedTime;
					}
					if (l_elapsedTime < l_min_elapsed_time) {
						l_min_elapsed_time = l_elapsedTime;
					}
				}	
			}

			conn.commit();
			
			System.out.println("****************** INSERT BENCHMARKS ***********************");
			System.out.println("Numero di INSERT eseguiti:" + ai_max_rows_inserted);
			System.out.println("COMMIT ogni " + ai_max_rows_per_commit + " righe inserite.");
			if (abSequentialUpdate) {
				System.out.println("INSERT SEQUENZIALI (uno per ogni iterazione) ");	
			} else {
				System.out.println("INSERT A BLOCCHI (batch - "+ai_max_rows_per_commit+" INSERT statement per volta).");
			}	
			
			System.out.println("Max insert time ns:" + l_max_elapsed_time);
			System.out.println("Max insert time ms:" + l_max_elapsed_time/1000000);
			
			System.out.println("Min insert time ns:" + l_min_elapsed_time);
			System.out.println("Min insert time ms:" + l_min_elapsed_time/1000000);			
			
			System.out.println("Total insert time ns:" + l_tot_elapsed_time);
			System.out.println("Total insert time ms:" + l_tot_elapsed_time/1000000);
			
			if (l_count > 0) {
				ld_average_time=l_tot_elapsed_time / l_count;
			}	
			System.out.println("Average time ns:" + ld_average_time);
			System.out.println("Average time ms:" + ld_average_time/1000000);

		} catch (SQLException e) {

			System.out.println(e.getMessage());
			throw e;

		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}
			
			conn.setAutoCommit(true);
		}
	}
	
  /**
   * Questo metodo è usato per eseguire select per PK sulla tabella creata per i benchmark.
   * 
   * @param conn --> connessione al DB
   * @param asTableName --> Nome della tabella
   * @param ai_PK_value --> valore della PK
   *
   */
	private void SelectFromTablebyPK (Connection conn, String asTableName, int ai_PK_value) throws SQLException {
		
		ResultSet rs = null;
		PreparedStatement preparedStatement = null;
		
		try {
			String selectSQL = "SELECT id, name, surname, created_date  FROM " + asTableName + " WHERE id = ?";	

			preparedStatement = conn.prepareStatement(selectSQL);

			preparedStatement.setInt(1, ai_PK_value);

			rs = preparedStatement.executeQuery();
			
			 while (rs.next()) {
				int id = rs.getInt(1);
				String name = rs.getString(2);
				String surname = rs.getString(3);
				Date ldDate = rs.getDate(4);
			 }

		}
		catch (SQLException e) {

			System.out.println(e.getMessage());
			throw e;
		}
		finally {
			if (rs != null) rs.close(); 
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}			
	}
	
  /**
   * Questo metodo è usato per eseguire i benchmark relativi
   * alle select per PK. Vengono calcolati il tempo massimo,
   * minimo e medio in nsec e msec.   
   * 
   * @param conn --> connessione al DB
   * @param asTableName --> Nome della tabella
   * @param ai_max_rows_inserted --> numero massimo di select da eseguire									
   *
   */
	public void SelectFromTableTime (Connection conn, String asTableName, int ai_max_rows_inserted) throws SQLException {
		
		long l_startTime=0;
		long l_elapsedTime=0;
		long l_min_elapsed_time=Long.MAX_VALUE;
		long l_max_elapsed_time=0;
		double ld_average_time=0;
		long l_tot_elapsed_time=0;
		int l_count=0;
		
		
		try {
			
			for(int i=1; i <= ai_max_rows_inserted; i++) {
				l_count++;
				l_startTime = System.nanoTime();
				SelectFromTablebyPK (conn, asTableName, i);
				l_elapsedTime = System.nanoTime() - l_startTime;
								
				l_tot_elapsed_time+=l_elapsedTime;
				
				if (l_elapsedTime > 0) {
					if (l_elapsedTime > l_max_elapsed_time) {
						l_max_elapsed_time = l_elapsedTime;
					}
					if (l_elapsedTime < l_min_elapsed_time) {
						l_min_elapsed_time = l_elapsedTime;
					}
				}
			}
			
			System.out.println("****************** SELECT BENCHMARKS ***********************");
			System.out.println("Numero di select eseguite:" + ai_max_rows_inserted);
			System.out.println("Max select time ns:" + l_max_elapsed_time);
			System.out.println("Max select time ms:" + l_max_elapsed_time/1000000);
			
			System.out.println("Min select time ns:" + l_min_elapsed_time);
			System.out.println("Min select time ms:" + l_min_elapsed_time/1000000);			
			
			System.out.println("Total select time ns:" + l_tot_elapsed_time);
			System.out.println("Total select time ms:" + l_tot_elapsed_time/1000000);
			
			if (l_count > 0) {
				ld_average_time=l_tot_elapsed_time / l_count;
			}	
			System.out.println("Average time ns:" + ld_average_time);
			System.out.println("Average time ms:" + ld_average_time/1000000);
		}	
		catch (SQLException e) {

			System.out.println(e.getMessage());
			throw e;
		}			
	}
	
 /**
   * Questo metodo è usato chiudere la connessione al DB
   * 
   * @param conn --> connessione al DB
   *
   */	
	public void CloseConn (Connection conn) throws Exception {
		
		try { if (conn != null) conn.close(); } catch (Exception e) {System.out.println(e.getMessage()); throw e;};
	}	
} 


/**
 * La classe App contiene il main dell'applicazione ed esegue
 * i benchmark di INSERT e SELECT su un DB PostGreSQL via JDBC
 *  
 * <p>L'applicazione esegue i seguenti step:
 *
 * <ul><li>1) carica da un file di properties (config.properties) gli estremi
 *    di connessione al DB e i parametri per il benchmark. In particolare vengono recuperati 
 *	  parametri per la costruzione dell'URL di connessione via JDBC.</li></ul>
 *
 *	  <ul><li><b>Dettaglio Parametri</b><ul><li> a) Il nome del DB </li>
 *	  	  <li> b) Lo User di default per la connessione con privilegi di admin sul DB</li>
 *    	  <li> c) la pwd di connessione al DB </li>
 *    	  <li> d) l'hostname del DBserver </li>
 *    	  <li> e) il prefisso dell'URL di connessione (es. jdbc:postgresql://) </li>
 *	  	  <li> f) il nome del class driver JDBC per connettersi al DB (es. org.postgresql.Driver) </li>
 *	  	  <li> g) Numero massimo di statement di INSERT per ogni COMMIT </li>
 *    	  <li> h) Numero massimo di righe da inserire in tabella durante il test </li></ul></li></ul>
 *
 *    <ul><li><p>I parametri a), b) e c) possono essere forniti anche dalla linea di comando
 *	  nel momento del lancio dell'applicazione seguendo il suddetto ordine. Nel caso
 *    non vengano specificati da linea di comando, i relativi valori di default
 *	  vengono recuperati dal file di properties (<b>config.properties</b>)</li></ul> 
 *	 
 * 	  <ul><li><p>2) Viene effettuata la connessione al DB utilizzando i parametri recuperati al
 *	  punto precedente.</li></ul>
 *
 * 	  <ul><li><p>3) Se la connessione avviene con successo, viene istanziato un oggetto della classe
 *	  <b>DBOperation</b> per eseguire le seguenti operazioni di benchmark</li>
 * 
 *	  	  <li><b>Creazione tabella di test</b> <ul><li>3.1) Invocazione del metodo createTable dell'oggetto per creare la tabella di test
 *		   sul DB chiamata <b>TEST_TABLE</b>. Se la tabella esiste gia' viene droppata per ricominciare
 *		   un nuovo test da zero. La tabella creata come esempio ha 4 colonne
 *				<ul><li> ID (PK di tabella) di tipo INTEGER </li>
 *				<li>SURNAME VARCHAR(255)</li>
 *				<li>NAME VARCHAR2(255)</li>
 *				<li>CREATED_DATE DATE</li></ul></li></ul></li>
 *	  
 *
 *	  <li><b>INSERT Benchmarks</b><ul><li>3.2) Se la creazione della tabella di test (<b>TEST_TABLE</b>) e' avvenuta con successo al punto precedente,
 *		   viene invocato il metodo <b>InsertIntoTable</b> dell'oggetto della classe <b>DBOperation</b> istanziato in
 *		   precedenza. Il metodo viene invocato passando in input
 *				<ul><li> a) la connessione creata (oggetto di tipo <b>Connection</b>)</li>
 *				<li> b) il nome della tabella sulla quale effettuare i benchmark (<b>TEST_TABLE</b> nel nostro caso)</li>
 *				<li> c) il numero di insert statement prima di ogni commit </li>
 *				<li> d) il numero massimo di righe da inserire in tabella </li>
 *				<li> e) un parametro boolean (abSequentialUpdate) che indica se effettuare le INSERT in modalita' sequenziale,
 *				   	ovvero una per ogni iterazione del ciclo di <b>INSERT STATEMENT</b>; oppure se effettuare le insert a blocchi
 *				   	in modalita' batch, con commit ogni X righe. Il metodo stampa a video i tempi in nanosecondi e msec (minimo, medio e massimo) 
 *				   	delle due modalita' sopra elencate. Vengono effettuati i commit su DB ogni X statement.</li></ul></li></ul>
 *		   
 *
 *	  <li><b>SELECT Benchmarks</b><ul><li>3.3) Se la creazione della tabella <b>TEST_TABLE</b> e' avvenuta con successo al punto 3.1) viene invocato il metodo
 *		   <b>SelectFromTableTime</b> dell'oggetto della classe <b>DBOperation</b> precedentemente istanziato. Il metodo  prevede in input
 *			   <ul><li>a) la connessione creata (oggetto di tipo <b>Connection</b>)</li>
 *			   <li>b) il nome della tabella sulla quale effettuare i benchmark (<b>TEST_TABLE</b> nel nostro caso)</li>
 *			   <li>c) il numero di select consecutive per PK da eseguire (nel nostro caso pari al numero delle righe inserite in
 *				  tabella al punto 3.2)</li></ul>
 *
 *		   Il metodo stampa a video i tempi in nanosecondi e msec (minimo, medio e massimo) delle select eseguite su DB.</li></ul></li></ul>
 *
 *	<ul><li>4) Se esiste una connessione valida creata al punto 1) viene chiusa.</li></ul>
 *
 *	@author Ivan Collinelli
 *
 */  
public class App 
{
	
    public static void main( String[] args )
    {
        
		final int MAX_NUM_INSERT_SQL = 2000000; // limite massimo di insert SQL permesse per ogni test
		final int MAX_NUM_ROWS_PER_COMMIT = 500000; // limite massimo di righe previste per COMMIT;
		
		// 1. get properties
		// Recupera i parametri di setup dell'applicazione
		
		Properties props = new Properties();
		InputStream inputSt = null;
		
		// Parametri connessione al DB
		String lsDatabase="";
		String lsDBbUser="";
		String lsDbPassword="";
		String lsHost="";
		String lsJdbcURL="";
		String lsClassDriverJDBC="";
		
		boolean lbOk=true;
		boolean lb_exists_table=true;
		Connection con = null;
		
		// oggetto che contiene i metodi di accesso al DB e relative operazioni (benchmarks INSERT/SELECT)
		DBOperation DBOps= null;
		int li_maxRowsPerCommit=0;
		int li_maxRowsInserted=0;
		
		System.out.println( "Tentativo di connessione al DB..." );						
		
		try {

			inputSt = new FileInputStream("src/main/resources/config.properties");
			
			// Caricamento file di configurazione che contiene i parametri dell'applicazione
			props.load(inputSt);
			
			// Parametri di connessione al DB	
			
			// recupero il nome del Database, lo user e la pwd dalla riga di comando se presenti
			// in caso negativo vengono impostati per i 3 parametri i valori di default specificati nel file config.properties
			
			for (int i=0; i<args.length && i<=2; i++) {
				switch (i) {
					case 0:
						lsDatabase=args[i];
						break;
					case 1:
						lsDBbUser=args[i];
						break;
					case 2:
						lsDbPassword=args[i];
						break;	
				}				
			}	

			// se non passati da linea di comando Database name, DBUser e DBPassword vengono recuperati dal file di properties
			if (lsDatabase=="") lsDatabase = props.getProperty("database");
			if (lsDBbUser=="") lsDBbUser = props.getProperty("dbuser");
			if (lsDbPassword=="") lsDbPassword = props.getProperty("dbpassword");

			lsHost = props.getProperty("host");
			lsJdbcURL = props.getProperty("jdbcURL");
			lsClassDriverJDBC = props.getProperty("ClassDriverJDBC");
			
			try {
				// Numero di statement eseguiti prima di ogni commit
				li_maxRowsPerCommit = Integer.parseInt(props.getProperty("maxRowsPerCommit"));
				// Numero massimo di righe da inserire
				li_maxRowsInserted = Integer.parseInt(props.getProperty("maxRowsInserted"));
				} 
			catch (Exception err) {
				// verifica che il valore numerico specificato nel file di configurazione non ecceda la capacità degli int
				System.out.println("ERROR: " + err);
				lbOk=false;
			} 

			if (li_maxRowsPerCommit > MAX_NUM_ROWS_PER_COMMIT || li_maxRowsInserted > MAX_NUM_INSERT_SQL ) {
				System.out.println("Il numero di insert previste oppure il numero di statement consentite prima di un commit supera il massimo consentito");
				lbOk=false;
			}		
			
			if (lbOk) {	
				System.out.println("Database Name:" + lsDatabase);
				System.out.println("Database User:" + lsDBbUser);
				System.out.println("Database Pwd:" + lsDbPassword);
				System.out.println("Database Host:" + lsHost);
				System.out.println("JDBC URL:" + lsJdbcURL);
				System.out.println("Max rows per commit:" + li_maxRowsPerCommit);
				System.out.println("Max rows inserted:" + li_maxRowsInserted);
			}

		} catch (IOException ex) {
			ex.printStackTrace();
			lbOk=false;
		} finally {
			if (inputSt != null) {
				try {
					inputSt.close();
				} catch (IOException e) {
					e.printStackTrace();
					lbOk=false;
				}
			}
		}
		
		if (lbOk) {
			// 2. Connessione al DB
			try {
				
				 String url = lsJdbcURL + lsHost + "/" + lsDatabase; 	
				 System.out.println("url DB:" + url);	
				 Class.forName(lsClassDriverJDBC);
				 con = DriverManager.getConnection(url, lsDBbUser,lsDbPassword);
				
				 System.out.println( "Connessione DB OK!" );
				
				}
				catch (Exception err) {
					System.out.println("ERROR: " + err);
					lbOk=false;
				}
		}
		
		
		if (lbOk) {
			// 3. Verifica esistenza della tabella sul DB collegato
			
			try {
				DBOps = new DBOperation();
				lb_exists_table = DBOps.tableExist(con,"test_table");
				} catch (SQLException e) {
					System.out.println(e.getMessage());
					lbOk=false;
				}
			
		}
		
		if (lbOk) {
			// se esiste la tabella viene droppata per ricominciare un nuovo test da zero
						
			try {
				if (lb_exists_table) {	
					System.out.println("La tabella TEST_TABLE esiste gia'. Ora viene droppata per un nuovo test JDBC benchmarks.");					
					DBOps.DropTable(con,"TEST_TABLE");
					System.out.println("Tabella TEST_TABLE droppata!");						
				}				

				DBOps.createTable(con,"TEST_TABLE");
				System.out.println("Tabella TEST_TABLE creata per nuovo test benchmark JDBC.");				
					
				
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				lbOk=false;
			}	
		}
		
		if (lbOk) {
			// insert con commit ogni X statements
			try {
				System.out.println("Inizio inserimento in tabella... ");
				// benchmark INSERT STATEMENT a blocchi di li_maxRowsPerCommit INSERT (modalità addBatch)
				DBOps.InsertIntoTable(con, "TEST_TABLE", li_maxRowsPerCommit, li_maxRowsInserted, false);
				// benchmark INSERT STATEMENT con insert sequenziali (no modalità batch)
				DBOps.DropTable(con, "TEST_TABLE");
				DBOps.createTable(con,"TEST_TABLE");
				DBOps.InsertIntoTable(con, "TEST_TABLE", li_maxRowsPerCommit, li_maxRowsInserted, true);
				System.out.println("Fine inserimento in tabella... ");

			} catch (SQLException e) {
				System.out.println(e.getMessage());
				lbOk=false;
			}	
					
		}
		
		if (lbOk) {
			// select statements (calcolo tempo medio, minimo e massimo)
			try {
				System.out.println("Inizio select in tabella per PK... ");
				DBOps.SelectFromTableTime(con, "TEST_TABLE", li_maxRowsInserted);
				System.out.println("Fine benchmark select");
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				lbOk=false;
			}
		}
		
		// chiusura connessione DB
		try {
			DBOps.CloseConn(con);	
		} catch (Exception e) {
			System.out.println(e.getMessage());
			lbOk=false;
		}				
			
			
    }
		
	
}
