package com.hp.hpit.DeviceEvent;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Random;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
 
public class MyVerticaConfiguration {
	/** JDBC logging level default */
	public static final String LOG_LEVEL_PROP_DEFAULT = "0";
	
	/** Class name for Vertica JDBC Driver */
	public static final String VERTICA_DRIVER_CLASS = "com.vertica.jdbc.Driver";
	public static final String VERTICA_DRIVER_CLASS_41 = "com.vertica.Driver";

	/** Host names to connect to, selected from at random */
	public static final String HOSTNAMES_PROP = "mapred.vertica.hostnames";

	/** Name of database to connect to */
	public static final String DATABASE_PROP = "mapred.vertica.database";

	/** User name for Vertica */
	public static final String USERNAME_PROP = "mapred.vertica.username";

	/** Password for Vertica */
	public static final String PASSWORD_PROP = "mapred.vertica.password";

	/** Port for Vertica */
	public static final String PORT_PROP = "mapred.vertica.port";
	
	/** JDBC logging level */
	public static final String LOG_LEVEL_PROP = "mapred.vertica.log.level";
	
	/**Batch Size for JDBC batch size */
	public static final String BATCH_SIZE_PROP = "mapred.vertica.batchsize";

	/** Host names to connect to, selected from at random */
	public static final String OUTPUT_HOSTNAMES_PROP = "mapred.vertica.hostnames.output";

	/** Name of database to connect to */
	public static final String OUTPUT_DATABASE_PROP = "mapred.vertica.database.output";

	/** User name for Vertica */
	public static final String OUTPUT_USERNAME_PROP = "mapred.vertica.username.output";

	/** Password for Vertica */
	public static final String OUTPUT_PASSWORD_PROP = "mapred.vertica.password.output";

	/** Password for Vertica */
	public static final String OUTPUT_PORT_PROP = "mapred.vertica.port.output";
	
	/** Query to run for input */
	public static final String QUERY_PROP = "mapred.vertica.input.query";

	/** Query to run to retrieve parameters */
	public static final String QUERY_PARAM_PROP = "mapred.vertica.input.query.paramquery";

	/** Static parameters for query */
	public static final String QUERY_PARAMS_PROP = "mapred.vertica.input.query.params";

	/** Optional input delimiter for streaming */
	public static final String INPUT_DELIMITER_PROP = "mapred.vertica.input.delimiter";

	/** Optional input terminator for streaming */
	public static final String INPUT_TERMINATOR_PROP = "mapred.vertica.input.terminator";

	/** Output table name */
	public static final String OUTPUT_TABLE_NAME_PROP = "mapred.vertica.output.table.name";

	/** Definition of output table types */
	public static final String OUTPUT_TABLE_DEF_PROP = "mapred.vertica.output.table.def";

	/** Whether to drop tables */
	public static final String OUTPUT_TABLE_DROP = "mapred.vertica.output.table.drop";

	/** Optional output format delimiter */
	public static final String OUTPUT_DELIMITER_PROP = "mapred.vertica.output.delimiter";

	/** Optional output format terminator */
	public static final String OUTPUT_TERMINATOR_PROP = "mapred.vertica.output.terminator";

	/** Optional log path, defaults to working directory */
	public static final String LOG_PATH_PROP = "mapred.vertica.log.path";

	/** Optional override of the default OutputCommitter implementation */
	public static final String OUTPUT_COMMITTER_CLASS_PARAM = "mapred.vertica.output.committer";

	/**
	 * Override the sleep timer for optimize to poll when new projections have
	 * refreshed
	 */
	public static final String OPTIMIZE_POLL_TIMER_PROP = "mapred.vertica.optimize.poll";

	/**
	 * Property for speculative execution of MAP tasks
	 */
	public static final String MAP_SPECULATIVE_EXEC = "mapred.map.tasks.speculative.execution";

	/**
	 * Property for speculative execution of MAP tasks
	 */
	public static final String REDUCE_SPECULATIVE_EXEC = "mapred.reduce.tasks.speculative.execution";
	
	public static Connection connection=null;
	
	/**
	  * Sets the Vertica database connection information in the (@link
	  * Configuration)
	  * 
	  * @param conf
	  *          the configuration
	  * @param hostnames
	  *          one or more hosts in the Vertica cluster
	  * @param database
	  *          the name of the Vertica database
	  * @param username
	  *          Vertica database username
	  * @param password
	  *          Vertica database password
	  * @param port
	  *          Vertica database port         
	  */
	public static void configureVertica(Configuration conf, String[] hostnames,
      String database, String port, String username, String password) {
		conf.setBoolean(MAP_SPECULATIVE_EXEC, false);
		conf.setBoolean(REDUCE_SPECULATIVE_EXEC, false);

		conf.setStrings(HOSTNAMES_PROP, hostnames);
		conf.set(DATABASE_PROP, database);
		conf.set(USERNAME_PROP, username);
		conf.set(PASSWORD_PROP, password);
		conf.set(PORT_PROP, port);
	}
  
	/**
	  * Sets the Vertica database connection information in the (@link
	  * Configuration)
	  * 
	  * @param conf
	  *          the configuration
	  * @param hostnames
	  *          one or more hosts in the Vertica cluster
	  * @param database
	  *          the name of the Vertica database
	  * @param username
	  *          Vertica database username
	  * @param password
	  *          Vertica database password
	  * @param port
	  *          Vertica database port
	  * @param level
	  *          JDBC logging level
	  *          
	  */
	public static void configureVertica(Configuration conf, String[] hostnames,
			String database, String port, String username, String password, String level) {
		configureVertica(conf, hostnames, database, port, username, password, level);
		conf.set(LOG_LEVEL_PROP, level);
	}
	
	/**
	  * Sets the Vertica database connection information in the (@link
	  * Configuration)
	  * 
	  * @param conf
	  *          the configuration
	  * @param hostnames
	  *          one or more hosts in the Vertica cluster
	  * @param database
	  *          the name of the Vertica database
	  * @param username
	  *          Vertica database username
	  * @param password
	  *          Vertica database password
	  * @param port
	  *          Vertica database port
	  * @param level
	  *          JDBC logging level
	  * @param logpath
	  * 		 JDBC log path - location of logs to be written, default is current directory
	  *          
	  */
	public static void configureVertica(Configuration conf, String[] hostnames,
			String database, String port, String username, String password, String level, String logpath) {
		configureVertica(conf, hostnames, database, port, username, password, level, logpath);
		conf.set(LOG_PATH_PROP, logpath);
	}

	/**
	  * Sets the Vertica database connection information in the (@link
	  * Configuration)
	  * 
	  * @param conf
	  *          the configuration
	  * @param hostnames
	  *          one or more hosts in the source Cluster
	  * @param database
	  *          the name of the source Vertica database
	  * @param username
	  *          for the source Vertica database
	  * @param password
	  *          for the source Vertica database
	  * @param port
	  *          for the source Vertica database
	  * @param level
	  *          JDBC logging level        
	  * @param logpath
	  *          JDBC deug logging  - location of logs to be written, default is current directory          
	  * @param output_hostnames
	  *          one or more hosts in the output Cluster
	  * @param output_database
	  *          the name of the output VerticaDatabase
	  * @param output_username
	  *          for the target Vertica database
	  * @param output_password
	  *          for the target Vertica database
	  * @param output_port
	  *          for the target Vertica database         
	  */
	public static void configureVertica(Configuration conf, String[] hostnames,
			String database, String port, String username, String password, String level, String logpath,
			String[] output_hostnames, String output_database, String output_port,
			String output_username, String output_password) {
		configureVertica(conf, hostnames, database, port, username, password, level , logpath);
		conf.setStrings(OUTPUT_HOSTNAMES_PROP, output_hostnames);
		conf.set(OUTPUT_DATABASE_PROP, output_database);
		conf.set(OUTPUT_PORT_PROP, output_port);
		conf.set(OUTPUT_USERNAME_PROP, output_username);
		conf.set(OUTPUT_PASSWORD_PROP, output_password);
	}

	private  Configuration conf;

	// default record terminator for writing output to Vertica
	public static final String RECORD_TERMINATOR = "\u0008";

	// default delimiter for writing output to Vertica
	public static final String DELIMITER = "\u0007";

	// defulat optimize poll timeout
	public static final int OPTIMIZE_POLL_TIMER = 1000;

	public static final int defaultBatchSize = 10000;

	public MyVerticaConfiguration(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConfiguration() {
		return conf;
	}

	/**
	  * Returns a connection to a random host in the Vertica cluster
	  * 
	  * @param output
	  *          true if the connection is for writing
	  * @throws IOException
	  * @throws ClassNotFoundException
	  * @throws SQLException
	  */
	public  static Connection getConnection(Configuration conf,boolean output) throws IOException,
      ClassNotFoundException, SQLException {
		
		if(connection!=null) return connection; 
		
    	try {
			Class.forName(VERTICA_DRIVER_CLASS);
		} catch (ClassNotFoundException e) {
			try {
				Class.forName(VERTICA_DRIVER_CLASS_41);
			} catch (ClassNotFoundException e2) {
				throw new RuntimeException(e);
			}
		}

		String[] hosts = conf.getStrings(HOSTNAMES_PROP);
		String user = conf.get(USERNAME_PROP);
		String pass = conf.get(PASSWORD_PROP);
		String database = conf.get(DATABASE_PROP);
		String port = conf.get(PORT_PROP);
		String level = conf.get(LOG_LEVEL_PROP);
		//String logpath = conf.get(LOG_PATH_PROP);
    

		if((level == null) || (level == "")){
			level = LOG_LEVEL_PROP_DEFAULT;
		}

		if (output) {
			hosts = conf.getStrings(OUTPUT_HOSTNAMES_PROP, hosts);
			user = conf.get(OUTPUT_USERNAME_PROP, user);
			pass = conf.get(OUTPUT_PASSWORD_PROP, pass);
			database = conf.get(OUTPUT_DATABASE_PROP, database);
			port = conf.get(OUTPUT_PORT_PROP, port);
		}

		if (hosts == null)
			throw new IOException("Vertica requies a hostname defined by "
					+ HOSTNAMES_PROP);
		if (hosts.length == 0)
			throw new IOException("Vertica requies a hostname defined by "
					+ HOSTNAMES_PROP);
		if (database == null)
			throw new IOException("Vertica requies a database name defined by "
					+ DATABASE_PROP);

		Random r = new Random();
		if (user == null)
			throw new IOException("Vertica requires a username defined by "
					+ USERNAME_PROP);

		if (port == null)
			throw new IOException("Vertica requires a port defined by "
					+ PORT_PROP);
		Connection connection = DriverManager.getConnection("jdbc:vertica://"
				+ hosts[r.nextInt(hosts.length)] + ":" + port + "/" + database + "?loglevel=" + level, 
				 user, pass);

		// if output is being written auto-commit must be disabled to prevent individual batches within
		// a task from being committed. Instead we want all the batches in a task to be committed or
		// rolled back upon task success or failure
		if (output) {
		    connection.setAutoCommit(false);
		   // connection.setAutoCommit(true);
		}

		return connection;
	}

	public String getInputQuery() {
		return conf.get(QUERY_PROP);
	}

	/**
	 * get Run this query and give the results to mappers.
	 * 
	 * @param inputQuery
	 */
	public void setInputQuery(String inputQuery) {
		inputQuery = inputQuery.trim();
		if (inputQuery.endsWith(";")) {
			inputQuery = inputQuery.substring(0, inputQuery.length() - 1);
		}
		conf.set(QUERY_PROP, inputQuery);
	}

	/**
	 * Return the query used to retrieve parameters for the input query (if set)
	 * 
	 * @return Returns the query for input parameters
	 */
	public String getParamsQuery() {
		return conf.get(QUERY_PARAM_PROP);
	}

	/**
	 * Query used to retrieve parameters for the input query. The result set must
	 * match the input query parameters precisely.
	 * 
	 * @param segmentParamsQuery
	 */
	public void setParamsQuery(String segmentParamsQuery) {
		conf.set(QUERY_PARAM_PROP, segmentParamsQuery);
	}

	 
	 
	/**
	 * For streaming return the delimiter to separate values to the mapper
	 * 
	 * @return Returns delimiter used to format streaming input data
	 */
	public String getInputDelimiter() {
		return conf.get(INPUT_DELIMITER_PROP, DELIMITER);
	}

	/**
	 * @deprecated  As of release 1.5, this function is not called from the Java API.
	 * For streaming set the delimiter to separate values to the mapper
	 */
	@Deprecated
	public void setInputDelimiter(String delimiter) {
		conf.set(INPUT_DELIMITER_PROP, delimiter);
	}

	/**
	 * For streaming return the record terminator to separate values to the mapper
	 * 
	 * @return Returns recorder terminator for input data
	 */
	public String getInputRecordTerminator() {
		return conf.get(INPUT_TERMINATOR_PROP, RECORD_TERMINATOR);
	}

	/**
	 * @deprecated  As of release 1.5, this function is not called from the Java API.
	 * For streaming set the record terminator to separate values to the mapper
	 */
	@Deprecated
	public void setInputRecordTerminator(String terminator) {
		conf.set(INPUT_TERMINATOR_PROP, terminator);
	}

	/**
	 * Get the table that is the target of output
	 * 
	 * @return Returns table name for output
	 */
	public String getOutputTableName() {
		return conf.get(OUTPUT_TABLE_NAME_PROP);
	}

	/**
	 * Set table that is being loaded as output
	 * 
	 * @param tableName
	 */
	public void setOutputTableName(String tableName) {
		conf.set(OUTPUT_TABLE_NAME_PROP, tableName);
	}

	/**
	 * Return definition of columns for output table
	 * 
	 * @return Returns table definition for output table
	 */
	public String[] getOutputTableDef() {
		return conf.getStrings(OUTPUT_TABLE_DEF_PROP);
	}

	/**
	 * Set the definition of a table for output if it needs to be created
	 * 
	 * @param args
	 */
	public void setOutputTableDef(String... args) {
		if(args == null || args.length == 0 || Arrays.asList(args).contains(null)) return;
		conf.setStrings(OUTPUT_TABLE_DEF_PROP, args);
  	}
  
	/**
	 * Return the batch size for batch insert
	 * 
	 * @return Returns the batch size for batch insert
	 */
	public long getBatchSize() {
		return conf.getLong(BATCH_SIZE_PROP, defaultBatchSize);
	}

	/**
	 * Return whether output table is truncated before loading
	 * 
	 * @return Returns true if output table should be dropped before loading
	 */
	public boolean getDropTable() {
		return conf.getBoolean(OUTPUT_TABLE_DROP, false);
	}

	/**
	 * Set whether to truncate the output table before loading
	 * 
	 * @param drop_table
	 */
	public void setDropTable(boolean drop_table) {
		conf.setBoolean(OUTPUT_TABLE_DROP, drop_table);
	}

	/**
	 * For streaming return the delimiter used by the reducer
	 * 
	 * @return Returns delimiter to use for output data
	 */
	public String getOutputDelimiter() {
		return conf.get(OUTPUT_DELIMITER_PROP, DELIMITER);
	}

	 
	/**
	 * For streaming return the record terminator used by the reducer
	 * 
	 * @return Returns the record terminator for output data
	 */
	public String getOutputRecordTerminator() {
		return conf.get(OUTPUT_TERMINATOR_PROP, RECORD_TERMINATOR);
	}
 
}
