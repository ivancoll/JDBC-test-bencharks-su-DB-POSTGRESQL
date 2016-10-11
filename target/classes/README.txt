Applicazione java di esempio per misurare il tempo minimo, massimo, medio nelle operazioni di INSERT/SELECT su un DB POSTGRESQL
-------------------------------------------------------------------------------------------------------------------------------

E’ stata realizzata un’applicazione JAVA che esegue i seguenti benchmark su un DB JDBC compliant, nel caso specifico POSTGRESQL 9.4. 
L’applicazione si connette via JDBC al Database, crea una tabella di esempio chiamata TEST_TABLE ed esegue operazioni di UPDATE massivi e select 
per PK monitorando il tempo minimo, massimo e medio per ogni statement. Ad ogni lancio dell’applicazione la tabella viene droppata e ricreata.

E’ stato creato un package it.test.jdbcPostgreSQL contenente le classi App (che contiene il main dell’applicazione) e DBOperation.

La classe DBOperation contiene i metodi utilizzati per i benchmark di INSERT e SELECT su un DB JDBC compliant 
(nel nostro caso POSTGRESQL 9.4 installato localmente sulla macchina nella quale sono stati eseguiti i test)

- createTabl--> Esegue la creazione di una tabella di DB sulla quali eseguire i benchmark
- DropTable	--> Utilizzato per droppare la tabella e ricrearla ad ogni lancio dell'applicazione. 

- InsertIntoTable --> Utilizzato per eseguire i benchmark degli statement di INSERT con commit ogni X statement. Update sequenziali e a blocchi. 

- SelectFromTableTime --> Utilizzato per eseguire i benchmark degli statement di SELECT per PK della tabella creata in precedenza.


- CloseConn	--> Utilizzato per chiudere la connessione al DB


La classe App contiene il main dell'applicazione ed esegue i benchmark di INSERT e SELECT su un DB PostGreSQL via JDBC.
L'applicazione esegue i seguenti step:
1) carica da un file di properties (config.properties) gli estremi di connessione al DB e i parametri per il benchmark. In particolare vengono recuperati parametri per la costruzione dell'URL di connessione via JDBC, ovvero
 	a) Il nome del DB 
 	b) Lo User di default per la connessione con privilegi di admin sul DB
 	c) la pwd di connessione al DB
 	d) l'hostname del DBserver
 	e) il prefisso dell'URL di connessione (es. jdbc:postgresql://)
 	f) il nome del class driver JDBC per connettersi al DB (es. org.postgresql.Driver)
 	g) Numero massimo di statement di INSERT per ogni COMMIT
 	h) Numero massimo di righe da inserire in tabella durante il test
 I parametri a), b) e c) possono essere forniti anche dalla linea di comando nel momento del lancio dell'applicazione seguendo il suddetto ordine. Nel caso non vengano specificati da linea di comando, i relativi valori di default vengono recuperati dal file di properties (config.properties) 
 	 
2) Viene effettuata la connessione al DB utilizzando i parametri recuperati al punto precedente.
3) Se la connessione avviene con successo, viene istanziato un oggetto della classe DBOperation per eseguire le seguenti operazioni di benchmark 
	3.1) Invocazione del metodo createTable dell'oggetto per creare la tabella di test sul DB chiamata TEST_TABLE. Se la tabella esiste già viene droppata per ricominciare un nuovo test da zero. La tabella creata come esempio ha 4 colonne
 		- ID (PK di tabella) di tipo INTEGER
 		- SURNAME VARCHAR(255)
 		- NAME VARCHAR2(255)
 		- CREATED_DATE DATE
	3.2) Se la creazione della tabella di test (TEST_TABLE) è avvenuta con successo al punto precedente, viene invocato il metodo InsertIntoTable dell'oggetto della classe DBOperation istanziato in precedenza. Il metodo viene invocato passando in input
 		a) la connessione creata (oggetto di tipo Connection)
 		b) il nome della tabella sulla quale effettuare i benchmark (TEST_TABLE nel nostro caso)
 		c) il numero di insert statement prima di ogni commit
 		d) il numero massimo di righe da inserire in tabella
		e) un parametro boolean (abSequentialUpdate) che indica se effettuare le INSERT in modalità sequenziale, ovvero una per ogni iterazione del ciclo 
		di INSERT STATEMENT; oppure se effettuare le insert a blocchi in modalità batch, con commit ogni X righe.Il metodo stampa a video i tempi 
		in nanosecondi e msec (minimo, medio e massimo) delle due modalità sopra elencate. Vengono effettuati i commit su DB ogni X statement.
	3.3) Se la creazione della tabella TEST_TABLE è avvenuta con successo al punto 3.1) viene invocato il metodo SelectFromTableTime dell'oggetto della classe DBOperation precedentemente istanziato. Il metodo prevede in input
		a) la connessione creata (oggetto di tipo Connection)
		b) il nome della tabella sulla quale effettuare i benchmark (TEST_TABLE nel nostro caso)
		c) il numero di select consecutive per PK da eseguire (nel nostro caso pari al numero delle righe inserite in tabella al punto 3.2)

	Il metodo stampa a video i tempi in nanosecondi e msec (minimo, medio e massimo) delle select eseguite su DB.

4) Se esiste una connessione valida creata al punto 1) viene chiusa.


Strumenti utilizzati
--------------------

DBMS PostGreSQL 9.4
Java JDK 8
Maven per la creazione del progetto 


E’ stato creato un progetto MAVEN contenente la struttura dell’applicazione. E’ stato creato il package it.test.jdbcPostgreSQL contenente la main class App. 
Nella directory src/main/resources del progetto è stato inserito il file di configurazione config.properties che contiene i parametri di setup 
dell’applicazione, in particolare 

jdbcURL=jdbc:postgresql://  --> prefisso URL di connessione via ODBC al DB PostgreSQL
dbpassword=nadav200  --> pwd di connessione al DB
database=postgres --> Nome del DB installato sul DB server
dbuser=postgres --> User per la connessione al DB con privilegi di ADMIN
host=127.0.0.1:5432 --> host del DB server (nel caso in oggetto per I test il DB è stato installato localmente)
ClassDriverJDBC = org.postgresql.Driver --> class driver JDBC
maxRowsPerCommit = 5000 --> numero massimo di statement per ogni commit; es. commit ogni 5000 righe inserite)
maxRowsInserted = 1000000 --> numero massimo di righe da inserire per il test, il numero massimo consentito è stato limitato a 2000000 di righe.

Nella cartella src/main/resources/JDBC è stato inserito il jar postgresql-9.4.1211.jar relativo al driver JDBC di POSTGRESQL. 
Il jar va copiato in una cartella compresa nel classpath dell’applicazione. Nel caso della macchina con Windows 10 utilizzata per i test è 
stato copiato nella cartella C:\Program Files\Java\jre1.8.0_102\lib\ext (dove sono presenti anche i file del JDK 8).

TEST
----
Una volta installato PostGreSQL 9.4 sulla macchina utilizzata per i test, è stato creato il DB name postgres. Si è utilizzato il browser pgAdmin 
4 per visualizzare la struttura del DB e controllare la tabella creata dall’applicazione


Struttura del progetto MAVEN
----------------------------
	/jdbcPostgreSQL
		src/main/java/it/test/ jdbcPostgreSQL/App.java) (main class)
		target/ jdbcPostgreSQL-1.0-SNAPSHOT.jar (versione compilata dell’applicazione)
		pom.xml (contiene le informazioni del progetto)
		src/main/resources/config.properties (file di configurazione dell’applicazione) 

