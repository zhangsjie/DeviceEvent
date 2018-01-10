package com.vertica.hadoop;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Output formatter for loading data to Vertica
 *
 */
public class VerticaOutputFormat1 extends OutputFormat<Text, VerticaRecord1> {
	private static final Log LOG = LogFactory
			.getLog(VerticaOutputFormat1.class);

	/* JDBC connection is initialized on-demand and re-used */
	private Connection connection;

	/**
	 * Set the output table
	 *
	 * @param job
	 * @param tableName
	 */
	public static void setOutput(Job job, String tableName) {
		setOutput(job, tableName, false);
	}

	/**
	 * Set the output table and whether to truncate it before loading
	 *
	 * @param job
	 * @param tableName
	 * @param truncateTable
	 */
	public static void setOutput(Job job, String tableName,
			boolean truncateTable) {
		setOutput(job, tableName, truncateTable, (String[]) null);
	}

	/**
	 * Set the output table, whether to truncate it before loading if it already
	 * exists or create table specification if it doesn't exist with column
	 * definitions
	 *
	 * @param job
	 * @param tableName
	 * @param truncateTable
	 * @param tableDef
	 *            list of column definitions such as "foo int",
	 *            "bar varchar(10)"
	 */
	public static void setOutput(Job job, String tableName,
			boolean truncateTable, String... tableDef) {
		VerticaConfiguration1 vtconfig = new VerticaConfiguration1(
				job.getConfiguration());
		vtconfig.setOutputTableName(tableName);
		vtconfig.setOutputTableDef(tableDef);
		// the following performs truncate table and not drop table
		vtconfig.setDropTable(truncateTable);
	}

	/** {@inheritDoc} */
	public void checkOutputSpecs(JobContext context) throws IOException {
		if (context.getConfiguration().getBoolean("mapred.isnewapi", true))
			checkOutputSpecs2(new VerticaConfiguration1(
					context.getConfiguration()));
		else
			checkOutputSpecs(new VerticaConfiguration1(
					context.getConfiguration()));

	}

	public static void checkOutputSpecs(VerticaConfiguration1 vtconfig)
			throws IOException {
		Relation vTable = new Relation(vtconfig.getOutputTableName());
		if (vTable.isNull())
			throw new IOException(
					"Vertica output requires a table name defined by "
							+ VerticaConfiguration1.OUTPUT_TABLE_NAME_PROP);
		String[] def = vtconfig.getOutputTableDef();
		boolean dropTable = vtconfig.getDropTable();

		Statement stmt = null;
		//updated at 2015/1/13 to close the connection
		Connection conn = null;
		try {
			conn = vtconfig.getConnection(true);
			DatabaseMetaData dbmd = conn.getMetaData();
			ResultSet rs = dbmd.getTables(null, vTable.getSchema(),
					vTable.getTable(), null);
			boolean tableExists = rs.next();

			stmt = conn.createStatement();

			if (tableExists && dropTable) {
				stmt = conn.createStatement();
				stmt.execute("TRUNCATE TABLE "
						+ vTable.getQualifiedName().toString());
			}

			// create table if it doesn't exist
			if (!tableExists) {
				if (def == null)
					throw new RuntimeException(
							"Table "
									+ vTable.getQualifiedName().toString()
									+ " does not exist and no table definition provided");
				if (!vTable.isDefaultSchema()) {
					stmt.execute("CREATE SCHEMA IF NOT EXISTS "
							+ vTable.getSchema());
				}
				StringBuffer tabledef = new StringBuffer("CREATE TABLE ")
						.append(vTable.getQualifiedName().toString()).append(
								" (");
				for (String column : def)
					tabledef.append(column).append(",");
				tabledef.replace(tabledef.length() - 1, tabledef.length(), ")");
				stmt.execute(tabledef.toString());
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			//added at 2015/1/13 to close the connection
			if(conn != null)
			{
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
		}
	}

	/** {@inheritDoc} */
	public RecordWriter<Text, VerticaRecord1> getRecordWriter(
			TaskAttemptContext context) throws IOException {

		VerticaConfiguration1 config = new VerticaConfiguration1(
				context.getConfiguration());

		if (context.getConfiguration().getBoolean("mapred.isnewapi", false)) {
			String[] tables = config.getOutputTableNames();
			try {
				return new VerticaRecordWriter1(
						getConnection(context.getConfiguration()), tables,
						config.getBatchSize());
			} catch (SQLException e) {
				throw new IOException(e);
			}
		} else {
			String table = config.getOutputTableName();
			try {
				return new VerticaRecordWriter1(
						getConnection(context.getConfiguration()), table,
						config.getBatchSize());
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

	}

	/**
	 * Optionally called at the end of a job to optimize any newly created and
	 * loaded tables. Useful for new tables with more than 100k records.
	 *
	 * @param conf
	 * @throws Exception
	 */
	public static void optimize(Configuration conf) throws Exception {
		VerticaConfiguration1 vtconfig = new VerticaConfiguration1(conf);
		Connection conn = vtconfig.getConnection(true);

		// TODO: consider more tables and skip tables with non-temp projections
		Relation vTable = new Relation(vtconfig.getOutputTableName());
		Statement stmt = conn.createStatement();
		ResultSet rs = null;
		HashSet<String> tablesWithTemp = new HashSet<String>();

		// for now just add the single output table
		tablesWithTemp.add(vTable.getQualifiedName().toString());

		// map from table name to set of projection names
		HashMap<String, Collection<String>> tableProj = new HashMap<String, Collection<String>>();
		rs = stmt
				.executeQuery("select projection_schema, anchor_table_name, projection_name from projections;");
		while (rs.next()) {
			String ptable = rs.getString(1) + "." + rs.getString(2);
			if (!tableProj.containsKey(ptable)) {
				tableProj.put(ptable, new HashSet<String>());
			}

			tableProj.get(ptable).add(rs.getString(3));
		}

		for (String table : tablesWithTemp) {
			if (!tableProj.containsKey(table)) {
				throw new RuntimeException(
						"Cannot optimize table with no data: " + table);
			}
		}

		String designName = (new Integer(conn.hashCode())).toString();
		stmt.execute("select dbd_create_workspace('" + designName + "')");
		stmt.execute("select dbd_create_design('" + designName + "', '"
				+ designName + "')");
		stmt.execute("select dbd_add_design_tables('" + designName + "', '"
				+ vTable.getQualifiedName().toString() + "')");
		stmt.execute("select dbd_populate_design('" + designName + "', '"
				+ designName + "')");

		// Execute
		stmt.execute("select dbd_create_deployment('" + designName + "', '"
				+ designName + "')");
		stmt.execute("select dbd_add_deployment_design('" + designName + "', '"
				+ designName + "', '" + designName + "')");
		stmt.execute("select dbd_add_deployment_drop('" + designName + "', '"
				+ designName + "')");
		stmt.execute("select dbd_execute_deployment('" + designName + "', '"
				+ designName + "')");

		// Cleanup
		stmt.execute("select dbd_drop_deployment('" + designName + "', '"
				+ designName + "')");
		stmt.execute("select dbd_remove_design('" + designName + "', '"
				+ designName + "')");
		stmt.execute("select dbd_drop_design('" + designName + "', '"
				+ designName + "')");
		stmt.execute("select dbd_drop_workspace('" + designName + "')");
		//added at 2015/1/13 to close the connection
		if(conn != null)
		{
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/** (@inheritDoc) */
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {

		VerticaConfiguration1 config = new VerticaConfiguration1(
				context.getConfiguration());

		try {
			Class<?> outputCommitterClass = config.getOutputCommitterClass();
			LOG.info("Initializing OutputCommitter class "
					+ outputCommitterClass.getCanonicalName());
			return initializeOutputCommitter(outputCommitterClass,
					context.getConfiguration());
		} catch (ClassNotFoundException e) {
			throw new IOException(
					"Could not find OutputCommitter class confiugured as "
							+ VerticaConfiguration1.OUTPUT_COMMITTER_CLASS_PARAM,
					e);
		}
	}

	/**
	 * Given a class, initializes it and returns it. If the class is a subclass
	 * of
	 * 
	 * @{link AbstractVerticaOutputCommitter}, then the constructor that takes a
	 * @{link VerticaOutputFormat} object will be used. Alternatively, the class
	 *        can implement
	 * @{link OutputCommitter} directly.
	 * @param outputCommitterClass
	 * @param configuration
	 * @return
	 * @throws IOException
	 */

	private OutputCommitter initializeOutputCommitter(
			Class<?> outputCommitterClass, Configuration configuration)
			throws IOException {
		// If class extends AbstractVerticaOutputCommitter, create one of those
		// else it needs to be an instance of OutputCommitter
		// else throw exception
		if (AbstractVerticaOutputCommitter.class
				.isAssignableFrom(outputCommitterClass)) {

			// iterate through the constructors to find the one we're looking
			// for
			for (Constructor<?> constructor : outputCommitterClass
					.getDeclaredConstructors()) {
				if (constructor.getGenericParameterTypes().length == 1
						&& constructor.getGenericParameterTypes()[0]
								.equals(VerticaOutputFormat1.class)) {
					try {
						return (OutputCommitter) constructor.newInstance(this);
					} catch (Exception e) {
						throw new IOException(
								"Could not initialize OutputCommitter class "
										+ outputCommitterClass
												.getCanonicalName(),
								e);
					}
				}
			}

			throw new IOException(
					"Implementation of AbstractVerticaOutputCommitter must "
							+ "have a constructor that takes a VerticaOutputFormat object");
		} else if (OutputCommitter.class.isAssignableFrom(outputCommitterClass)) {
			return (OutputCommitter) ReflectionUtils.newInstance(
					outputCommitterClass, configuration);
		}

		throw new IOException(String.format(
				"Configured output committer class %s must implement %s",
				outputCommitterClass.getCanonicalName(),
				OutputCommitter.class.getCanonicalName()));
	}

	/**
	 * Initialize the connection when it's first requested and keep a handle to
	 * it.
	 *
	 * @param configuration
	 *            configuration object
	 * @return Connection to use
	 * @throws IOException
	 *             if connection can not be made
	 */
	synchronized Connection getConnection(Configuration configuration)
			throws IOException {
		if (connection != null) {
			return connection;
		}

		VerticaConfiguration1 config = new VerticaConfiguration1(configuration);
		try {
			LOG.info("Initializing new JDBC connection...");
			connection = config.getConnection(true);
			LOG.info("Initialized JDBC connection, autoCommit="
					+ connection.getAutoCommit());
			return connection;
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	public static void setOutputs(Job job, String tableNames,
			boolean truncateTable) {
		VerticaConfiguration1 vtconfig = new VerticaConfiguration1(
				job.getConfiguration());
		vtconfig.setOutputTableNames(tableNames);
		// the following performs truncate table and not drop table
		vtconfig.setDropTable(truncateTable);
	}

	public static void checkOutputSpecs2(VerticaConfiguration1 vtconfig)
			throws IOException {
		String[] tables = vtconfig.getOutputTableNames();
		if (tables.length == 0)
			throw new IOException(
					"Vertica output requires at least one table name defined by "
							+ VerticaConfiguration1.OUTPUT_TABLE_NAME_PROPS);

		
		for (String tablename : tables) {
			handleOneTable(vtconfig, tablename);			
		}

	}

	private static void handleOneTable(VerticaConfiguration1 vtconfig,
			String tablename) throws IOException {
		Relation vTable = new Relation(tablename);
		if (vTable.isNull())
			throw new IOException(
					"Vertica output requires a table name defined by "
							+ VerticaConfiguration1.OUTPUT_TABLE_NAME_PROP);

		boolean dropTable = vtconfig.getDropTable();

		Statement stmt = null;
		try {
			Connection conn = vtconfig.getConnection(true);
			DatabaseMetaData dbmd = conn.getMetaData();
			ResultSet rs = dbmd.getTables(null, vTable.getSchema(),
					vTable.getTable(), null);
			boolean tableExists = rs.next();

			stmt = conn.createStatement();

			if (tableExists && dropTable) {
				stmt = conn.createStatement();
				stmt.execute("TRUNCATE TABLE "
						+ vTable.getQualifiedName().toString());
			}

			if (!tableExists)
				throw new IOException(
						"table:"
								+ tablename
								+ " defined by "
								+ VerticaConfiguration1.OUTPUT_TABLE_NAME_PROP
								+ " is not exists in database,please check and create it");

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
		}
	}

}