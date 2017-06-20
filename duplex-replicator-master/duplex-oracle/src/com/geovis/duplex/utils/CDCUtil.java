package com.geovis.duplex.utils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.geovis.duplex.model.Column;

public class CDCUtil {
	/*	private static Properties prop = new Properties();
	static {
		try {
			File f = new File(System.getProperty("user.dir") + "/conf/cdc.param");
			InputStream in = new FileInputStream(f);
			prop.load(in);
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}*/

	/*	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName(prop.getProperty("cdc_driver"));
		Connection dbaConn = getDbaConnection(prop.getProperty("cdc_dburl"), prop.getProperty("dba_password"));
		initSystem(dbaConn);
//		createDirectory(dbaConn, prop.getProperty("cdc_directory"), prop.getProperty("cdc_directory_path"));
		createTableSpace(dbaConn, prop.getProperty("cdc_tablespace"), prop.getProperty("cdc_directory_path") + "/cdcuser.dbf", 2048, 2048);
		System.out.println("tablespace created!");
		createTempSpace(dbaConn, prop.getProperty("cdc_tempspace"), prop.getProperty("cdc_directory_path") + "/cdctemp.dbf", 2048, 2048);
		System.out.println("tempspace created!");
		createUser(dbaConn, prop.getProperty("cdc_user"), prop.getProperty("cdc_pwd"), prop.getProperty("cdc_tablespace"), prop.getProperty("cdc_tempspace"));
		System.out.println("cdcuser created!");
		grantBasePrivilege(dbaConn, prop.getProperty("cdc_user"));
		System.out.println("base privilege granted!");
		grantCDCPrivilege(dbaConn, prop.getProperty("cdc_user"));
		System.out.println("CDC privilege granted!");
		createChangeSet(getCDCConnection(), prop.getProperty("change_set"));
		System.out.println("change set created!");
		executePLSQL(getCDCConnection(), "dbms_cdc_publish.purge;");
	}*/

	public static void initSystem(Connection conn) throws SQLException{
		PreparedStatement stmt;
		ResultSet rs;
		stmt = conn.prepareStatement("select name,value from v$system_parameter where name=?");
		try {
			System.out.println("checking system parameter java_pool_size ...");
			stmt.setString(1, "java_pool_size");
			rs = stmt.executeQuery();
			if (rs.next() && rs.getInt(2) < 50000000) {
				execute(conn, "alter system set java_pool_size = 50000000");
			}
			rs.close();
		} catch (SQLException e) {
			System.out.println("alter system java_pool_sizes failed!");
		}

		try {
			System.out.println("checking system parameter job_queue_processes ...");
			stmt.setString(1, "job_queue_processes");
			rs = stmt.executeQuery();
			if (rs.next() && rs.getInt(2) < 1000) {
				execute(conn, "alter system set job_queue_processes = 1000");
			}
			rs.close();
		} catch (SQLException e) {
			System.out.println("alter system job_queue_processes failed!");
		}

		try {
			System.out.println("checking system parameter parallel_max_servers ...");
			stmt.setString(1, "parallel_max_servers");
			rs = stmt.executeQuery();
			if (rs.next() && rs.getInt(2) < 100) {
				execute(conn, "alter system set parallel_max_servers = 100");
			}
			rs.close();
		} catch (SQLException e) {
			System.out.println("alter system parallel_max_servers failed!");
		}

		try {
			System.out.println("checking system parameter processes ...");
			stmt.setString(1, "processes");
			rs = stmt.executeQuery();
			if (rs.next() && rs.getInt(2) < 1000) {
				execute(conn, "alter system set processes = 1000  scope=spfile");
			}
			rs.close();
		} catch (SQLException e) {
			System.out.println("alter system processes failed!");
		}

		try {
			System.out.println("checking system parameter streams_pool_size ...");
			stmt.setString(1, "streams_pool_size");
			rs = stmt.executeQuery();
			if (rs.next() && rs.getInt(2) < 67108864) {
				execute(conn, "alter system set streams_pool_size = 67108864");
			}
			rs.close();
		} catch (SQLException e) {
			System.out.println("alter system streams_pool_size failed!");
		}

		try {
			System.out.println("checking system parameter open_cursors ...");
			stmt.setString(1, "open_cursors");
			rs = stmt.executeQuery();
			if (rs.next() && rs.getInt(2) < 3000) {
				execute(conn, "alter system set open_cursors = 3000");
			}
			rs.close();
		} catch (SQLException e) {
			System.out.println("alter system open_cursors failed!");
		}

		try {
			execute(conn, "alter system set undo_retention = 3600");
		} catch (SQLException e) {
			System.out.println("alter system undo_retention failed!");
		}
		
		try {
			stmt.close();
		} catch (SQLException e) {
		}

		System.out.println("check completed !");
	}

	/*	public static Connection getDbaConnection() throws SQLException {
		try {
			Class.forName("oracle.jdbc.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		Properties info = new Properties();
		info.put("user","sys");
		info.put("password","123456");
		info.put("defaultRowPrefetch","15");
		info.put("internal_logon","sysdba");
		return DriverManager.getConnection("jdbc:oracle:thin:@192.168.4.231:1521:orcl", info);
	}

	 */
	public static Connection getDbaConnection(String url, String password) throws SQLException {
		Properties info = new Properties();
		info.put("user","sys");
		info.put("password", password);
		info.put("defaultRowPrefetch","15");
		info.put("internal_logon","sysdba");
		return DriverManager.getConnection(url, info);
	}

	public static void createDirectory(Connection dbaConn, String name, String path) 
			throws SQLException {
		StringBuilder sb = new StringBuilder("create directory ");
		sb.append(name);
		sb.append(" as '");
		sb.append(path);
		sb.append("'");
		execute(dbaConn, sb.toString());

	}

	public static void createTableSpace(Connection dbaConn, 
			String tablespaceName, String path, String size,
			String autoextendSize) throws SQLException{
		createTableSpace(dbaConn, tablespaceName, path, size, autoextendSize, false);
	}

	public static boolean existTablespace(Connection dbaConn, String tablespaceName) throws SQLException {
		System.out.println("checking tablespace " + tablespaceName + " ...");
		Statement stmt = dbaConn.createStatement();
		ResultSet rs = stmt.executeQuery("select tablespace_name from dba_tablespaces where tablespace_name ='" + tablespaceName.toUpperCase() + "'");
		boolean next = rs.next();
		rs.close();
		stmt.close();
		return next;
	}

	public static void createTempSpace(Connection dbaConn, 
			String tablespaceName, String path, String size,
			String autoextendSize) throws SQLException{
		createTableSpace(dbaConn, tablespaceName, path, size, autoextendSize, true);
	}

	public static void createTableSpace(Connection dbaConn, 
			String tablespaceName, String path, String size,
			String autoextendSize, boolean isTemp) throws SQLException{
		if (existTablespace(dbaConn, tablespaceName)) {
			return;
		}
		StringBuilder sb = new StringBuilder("create ");
		sb.append(isTemp ? "temporary tablespace " : "tablespace ")
		.append(tablespaceName)
		.append(isTemp ? " tempfile '" : " datafile '")
		.append(path) 
		.append("' size ")
		.append(size);

		if (autoextendSize != null && !autoextendSize.trim().equals("")) {
			sb.append(" autoextend on next  "); 
			sb.append(autoextendSize);
		}
		execute(dbaConn, sb.toString());
	}

	public static void dropTablespace(Connection dbaConn, String tablespace)
			throws SQLException {
		StringBuilder sb = new StringBuilder("drop tablespace ");
		sb.append(tablespace);
		sb.append(" including contents and datafiles");
		execute(dbaConn, sb.toString());
	}

	public static void createUser(Connection dbaConn, 
			String username, String password, 
			String tablespace, String tempTableSpace) throws SQLException {

		StringBuilder sb = new StringBuilder("create user ");
		sb.append(username);
		sb.append(" IDENTIFIED by ");
		sb.append(password);
		sb.append(" DEFAULT TABLESPACE ");
		sb.append(tablespace);

		if (tempTableSpace !=null && !tempTableSpace.trim().equals("")) {
			sb.append(" temporary tablespace "); 
			sb.append(tempTableSpace); 
			sb.append(" QUOTA UNLIMITED ON SYSTEM QUOTA UNLIMITED ON SYSAUX");
		}
		execute(dbaConn, sb.toString());
	}

	public static void grantCDCPrivilege(Connection dbaConn, String username) 
			throws SQLException{
		StringBuilder sb = new StringBuilder("GRANT CREATE SEQUENCE,");
		sb.append("CREATE JOB,CREATE SESSION,");
		sb.append(" CREATE TABLE,CREATE TABLESPACE, ");
		sb.append("UNLIMITED TABLESPACE,SELECT_CATALOG_ROLE,");
		sb.append("EXECUTE_CATALOG_ROLE,DBA,select any table,");
		sb.append("update any table, delete any table, insert any table to ");
		sb.append(username);
		execute(dbaConn, sb.toString());
		execute(dbaConn, "grant execute on dbms_cdc_publish to " + username);
	}

	public static void grantBasePrivilege(Connection dbaConn, String username) 
			throws SQLException{
		StringBuilder sb = new StringBuilder("grant create session,");
		sb.append(" create table, create tablespace ,create view ,");
		sb.append("connect,resource,select any dictionary  to ");
		sb.append(username);
		execute(dbaConn, sb.toString());
	}
	
	public static void purge(Connection conn) throws SQLException{
		executePLSQL(conn, "DBMS_CDC_PUBLISH.PURGE;");
	}

	public static void createChangeSet(Connection conn, String setName) 
			throws SQLException{
		StringBuilder sb = 
				new StringBuilder("DBMS_CDC_PUBLISH.CREATE_CHANGE_SET(change_set_name => '"); 
		sb.append(setName);
		sb.append("',description =>'new change set',change_source_name => 'SYNC_SOURCE');");
		executePLSQL(conn, sb.toString());
	}

	public static void enableChangeSet(Connection conn, String setName) 
			throws SQLException{
		StringBuilder sb = 
				new StringBuilder("DBMS_CDC_PUBLISH.ALTER_CHANGE_SET(change_set_name => '");
		sb.append(setName);
		sb.append("',enable_capture => 'y');");
		System.out.println(sb.toString());
		executePLSQL(conn, sb.toString());
	}

	public static void purgeChangeSet (Connection conn, String setName) 
			throws SQLException{
		StringBuilder sb = 
				new StringBuilder("DBMS_CDC_PUBLISH.PURGE_CHANGE_SET(change_set_name => '");
		sb.append(setName);
		sb.append("');");
		System.out.println(sb.toString());
		executePLSQL(conn, sb.toString());
	}

	public static void dropChangeSet (Connection conn, String setName) 
			throws SQLException{
		StringBuilder sb = 
				new StringBuilder("DBMS_CDC_PUBLISH.DROP_CHANGE_SET(change_set_name => '");
		sb.append(setName);
		sb.append("');");
		System.out.println(sb.toString());
		executePLSQL(conn, sb.toString());
	}

	public static void createChangeTable(Connection conn,String cdcuser, 
			String changeTableName, String setName, String sourceSchema,
			String sourceTable, String colTypeList, String captureValue, 
			String cdcTableSpace) throws SQLException {
		StringBuilder sb = new StringBuilder("DBMS_CDC_PUBLISH.CREATE_CHANGE_TABLE(owner => '");
		sb.append(cdcuser);
		sb.append("',change_table_name => '");
		sb.append(changeTableName);
		sb.append("',change_set_name => '");                                                                                                                                                                                                                                                                                          
		sb.append(setName);
		sb.append("',source_schema => '");
		sb.append(sourceSchema);
		sb.append("',source_table => '");
		sb.append(sourceTable);
		sb.append("',column_type_list => '");
		sb.append(colTypeList);
		sb.append("',capture_values => '");
		sb.append(captureValue);
		sb.append("',rs_id => 'n',row_id => 'n',user_id => 'y',timestamp => 'n',"
				+ "object_id => 'n',source_colmap => 'n',target_colmap => 'n'," + "DDL_MARKERS=>'n',"
				+ "options_string => 'TABLESPACE ");
		sb.append(cdcTableSpace);
		sb.append("');");
		System.out.println(sb.toString());
		executePLSQL(conn, sb.toString());
		execute(conn, "grant all on "+ changeTableName +" to " + sourceSchema);
	}

	public static void purgeChangeTable(Connection conn, String owner, String tableName) 
			throws SQLException{
		StringBuilder sb = new StringBuilder("DBMS_CDC_PUBLISH.PURGE_CHANGE_TABLE(owner => '");
		sb.append(owner);
		sb.append("',change_table_name=>'");
		sb.append(tableName);
		sb.append("');");
		executePLSQL(conn, sb.toString());
	}

	public static void dropChangeTable(Connection conn, String owner, String tableName, String cascade) 
			throws SQLException{
		StringBuilder sb = new StringBuilder("DBMS_CDC_PUBLISH.DROP_CHANGE_TABLE(owner=>'");
		sb.append(owner);
		sb.append("',change_table_name=>'");
		sb.append(tableName);
		sb.append("',force_flag=>'");
		sb.append(cascade);
		sb.append("');");
		executePLSQL(conn, sb.toString());
	}

	public static void createSubscription(Connection conn, String subscrpName, 
			String setName) throws SQLException {
		StringBuilder sb = new StringBuilder("DBMS_CDC_SUBSCRIBE.CREATE_SUBSCRIPTION("
				+ "change_set_name => '");
		sb.append(setName);
		sb.append("',description => 'Change data for PRODUCTS',subscription_name => '");
		sb.append(subscrpName);
		sb.append("');");
		executePLSQL(conn, sb.toString());
	}

	public static void subscribe(Connection conn, String subscrpName, String sourceSchema, 
			String sourceTable, String viewName, String columnList) throws SQLException{
		StringBuilder sb = new StringBuilder("DBMS_CDC_SUBSCRIBE.SUBSCRIBE("
				+ "subscription_name => '");
		sb.append(subscrpName);
		sb.append("',source_schema => '");
		sb.append(sourceSchema);
		sb.append("',source_table => '");
		sb.append(sourceTable);
		sb.append("',column_list => '");
		sb.append(columnList);
		sb.append("',subscriber_view => '");
		sb.append(viewName);
		sb.append("');");
		executePLSQL(conn, sb.toString());
	}

	public static void activeSubscrpt(Connection conn, String subscrpName) throws SQLException {
		executePLSQL(conn, "DBMS_CDC_SUBSCRIBE.ACTIVATE_SUBSCRIPTION("
				+ "subscription_name => '"+subscrpName+"');");
	}

	public static void extendWindow(Connection conn, String subscrpName) throws SQLException{
		executePLSQL(conn, "DBMS_CDC_SUBSCRIBE.EXTEND_WINDOW("
				+ "subscription_name => '"+subscrpName+"');");
	}

	public static void purgeWindow(Connection conn, String subscrpName) throws SQLException{
		executePLSQL(conn, "DBMS_CDC_SUBSCRIBE.PURGE_WINDOW("
				+ "subscription_name => '"+subscrpName+"');");
	}

	/**
	 * drop subscription with name subscrpName
	 * @param conn connection to CDC user
	 * @param subscrpName subscription name
	 * @throws SQLException 
	 */
	public static void dropSubscription(Connection conn, String subscrpName) throws SQLException {
		executePLSQL(conn, "DBMS_CDC_SUBSCRIBE.DROP_SUBSCRIPTION("
				+ "subscription_name => '"+subscrpName+"');");
	}

	public static void dropChangeTable(Connection conn, String owner, String tableName) 
			throws SQLException{
		dropChangeTable(conn, owner, tableName, "Y");
	}

	public static Connection getConnection(String url, String user, String password) 
			throws SQLException{
		return DriverManager.getConnection(url, user, password);
	}

	public static int executePLSQL(Connection conn, String script) 
			throws SQLException {
		StringBuilder sb = new StringBuilder("BEGIN \n");
		sb.append(script);
		sb.append("\n END; \n");
		return execute(conn, sb.toString());
	}

	public static boolean existSubscription(Connection conn, String name) {
		return exist(conn, "ALl_SUBSCRIPTIONS", "SUBSCRIPTION_NAME", name);
	}

	public static boolean existChangeTable(Connection conn, String name) {
		return exist(conn, "ALL_CHANGE_TABLES", "CHANGE_TABLE_NAME", name);
		
	}

	public static boolean existChangeSet(Connection conn, String name) {
		return exist(conn, "all_change_sets", "set_name", name);
	}

	public static int execute(Connection conn, String sql) 
			throws SQLException{
		Statement stmt = conn.createStatement();
		int count = stmt.executeUpdate(sql);
		try {
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}
	/**
	 * close the connection
	 * @param conn 
	 */
	public static void close(Connection conn){
		try {
			if(conn != null)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static boolean exist(Connection conn, String from, String condition, String value){
		StringBuilder sql = new StringBuilder("select * from ");
		sql.append(from).append(" where ").append(condition)
		.append( "='").append(value.toUpperCase()).append("'");
		boolean b = false;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			b = rs.next();
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return b;
	}

	public static boolean userExist(Connection dbaConn ,String username) throws SQLException {
		System.out.println("checking user " + username + " ...");
		Statement stmt = dbaConn.createStatement();
		ResultSet rs = stmt.executeQuery("select username from dba_users where username ='" + username.toUpperCase() + "'");
		boolean next = rs.next();
		rs.close();
		stmt.close();
		return next;
	}

	public static Map<String, Integer> getTypeCodes(Connection connection, String schema, String tablename) 
			throws SQLException {
		String sql = "select * from " + schema + "." + tablename + " where 0 > 1";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		ResultSetMetaData metaData = rs.getMetaData();
		int count = metaData.getColumnCount();
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (int i = 1; i <= count; i++) {
			map.put(metaData.getColumnName(i), metaData.getColumnType(i));
		}
		try {
			rs.close();
			statement.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}

	public static Map<String, Column> getTableColumns(Connection conn, String schema, String tablename) {
		Map<String, Column> columns = new HashMap<>();
		try {
			conn.setAutoCommit(false);
			DatabaseMetaData dm = conn.getMetaData();

			ResultSet rs = dm.getColumns(schema.toUpperCase(), schema.toUpperCase(), tablename.toUpperCase(), null);
			ResultSet primaryKeys = dm.getPrimaryKeys(schema.toUpperCase(), schema.toUpperCase(), tablename.toUpperCase());
			ResultSet importedKeys = dm.getImportedKeys(schema.toUpperCase(), schema.toUpperCase(), tablename.toUpperCase());
			Map<String, Integer> typeCodes = getTypeCodes(conn, schema.toUpperCase(), tablename.toUpperCase());

			while (rs.next()) {
				Column column = new Column();
				String columnName = rs.getString("COLUMN_NAME");
				column.setColumnName(columnName);
				column.setDataType(rs.getString("TYPE_NAME"));
				column.setLength(rs.getInt("COLUMN_SIZE"));
				column.setNullable(rs.getString("IS_NULLABLE").equals("YES"));
				column.setDataTypeCode(typeCodes.get(columnName));
				if (column.getLength() == 0) {
					if (column.getDataType().equalsIgnoreCase("NUMBER")) {
						column.setLength(22);
					}
				}
				columns.put(columnName, column);
			}
			rs.close();
			while (importedKeys.next()) {
				Column column = columns.get(importedKeys.getString("FKCOLUMN_NAME"));
				column.setFkColumnName(importedKeys.getString("FKCOLUMN_NAME"));
				column.setFkTableName(importedKeys.getString("FKTABLE_NAME"));
				column.setFkTableSchem(importedKeys.getString("FKTABLE_SCHEM"));
				column.setPkColumnName(importedKeys.getString("PKCOLUMN_NAME"));
				column.setPkTableName(importedKeys.getString("PKTABLE_NAME"));
				column.setPkTableSchema(importedKeys.getString("PKTABLE_SCHEM"));
			}
			importedKeys.close();
			while(primaryKeys.next()){
				Column column = columns.get(primaryKeys.getString("COLUMN_NAME"));
				column.setKey(true);
			}
			primaryKeys.close();

			String sql = "select * from " + schema + "." + tablename + " where 0 > 1";
			Statement statement = conn.createStatement();
			ResultSet rs1 = statement.executeQuery(sql);
			ResultSetMetaData metaData = rs1.getMetaData();
			int count = metaData.getColumnCount();
			for (int i = 1; i <= count; i++) {
				Column column = columns.get(metaData.getColumnName(i));
				column.setDataTypeCode(metaData.getColumnType(i));
			}
			rs1.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return columns;
	}
	
	public static boolean existView(Connection connection, String viewName) {
		StringBuilder sql = new StringBuilder("select view_name from user_views where view_name='");
		sql.append(viewName.toUpperCase()).append("'");
		boolean b = false;
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			b = rs.next();
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return b;
	}

	public static void main(String[] args) throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.4.199:1521:orcl", "cdcuser", "123456");
		dropChangeTable(conn, "cdcuser", "GFGXDB_JYW_GFGX_Y_YCFW_FWYXX");
		dropChangeTable(conn, "cdcuser", "GFGXDB_JYW_GFGX_Y_YCFW_WS");
		dropChangeTable(conn, "cdcuser", "GFGXDB_JYW_GFGX_Y_YCFW_WSNL");
		dropChangeTable(conn, "cdcuser", "GFGXDB_JYW_GFGX_Y_YCFW_YCCL");
	}

}
