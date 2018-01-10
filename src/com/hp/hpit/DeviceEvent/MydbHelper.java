package com.hp.hpit.DeviceEvent;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.mortbay.log.Log;

import com.vertica.hadoop.Relation;

public class MydbHelper {
	
public static final String LOG_LEVEL_PROP_DEFAULT = "0";
	
	static Connection connection = null;  
	private Configuration conf;
	PreparedStatement statement = null;
	Relation vTable = null;
	long batchSize = 10000;
	long numRecords = 0;
    
	
	public MydbHelper(){}
	public MydbHelper(Configuration _conf){conf=_conf; }
	public MydbHelper(Configuration _conf,String tablename) {
		conf=_conf;
		vTable = new com.vertica.hadoop.Relation(tablename);
	}
	
	//Set the configuration
	public void SetConn(Configuration _conf)
	{
		conf=_conf;
	}
	
	public void BuildStatement(String stmt) throws ClassNotFoundException, IOException, SQLException
	{
		if(connection==null) connection= MyVerticaConfiguration.getConnection(conf, true);
		statement = connection.prepareStatement(stmt);
	}
 
	public void write(MyVerticaRecord record) throws IOException {
	   
		try {
				record.write(statement);
				numRecords++;
			if (numRecords % batchSize == 0) {
				statement.executeBatch();
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
		
	}

	public void BuildStatementByTable(String tableName) throws ClassNotFoundException, IOException, SQLException
	{
		if(connection==null)connection= MyVerticaConfiguration.getConnection(conf, true);
		
		if(vTable==null) vTable = new com.vertica.hadoop.Relation(tableName);
		
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT /*DIRECT*/ INTO ");
		sb.append(vTable.getQualifiedName());

		StringBuilder values = new StringBuilder();
		values.append(" VALUES(");
		sb.append("(");

		String metaStmt = "select ordinal_position, column_name, data_type, is_identity, data_type_name " +
			"from v_catalog.odbc_columns " + 
			"where schema_name = ? and table_name = ? "
			+ "order by ordinal_position;";

		PreparedStatement stmt = connection.prepareStatement(metaStmt);
		stmt.setString(1, vTable.getSchema());
		stmt.setString(2, vTable.getTable());

		ResultSet rs = stmt.executeQuery();
		boolean addComma = false;
		while (rs.next()) {
			if (!rs.getBoolean(4)) {
				if (addComma) {
					sb.append(',');
					values.append(',');
				}
				sb.append(rs.getString(2));
				values.append('?');
				addComma = true;
			} 		}

		sb.append(')');
		values.append(')');
		sb.append(values.toString());

		statement = connection.prepareStatement(sb.toString());
		
	}
	
	public void close() throws IOException {
		try {
			   statement.executeBatch();
			   if(!connection.getAutoCommit())
				   connection.commit();
            } catch (SQLException e) {
			throw new IOException(e);
		}
	}

	public void ExecuteString(String sql)
	{
		try
		{
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.executeUpdate();
		}
		catch(SQLException e)
		{
			Log.info(e.getMessage());
		}
	}
}
