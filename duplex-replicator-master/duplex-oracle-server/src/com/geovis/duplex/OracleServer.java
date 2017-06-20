package com.geovis.duplex;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.geovis.duplex.driver.ConnectionFactory;
import com.geovis.duplex.extract.Extractor;
import com.geovis.duplex.model.Column;
import com.geovis.duplex.model.SchemaModel;
import com.geovis.duplex.model.TableModel;
import com.geovis.duplex.task.AbstractTask;
import com.geovis.duplex.task.Task;
import com.geovis.duplex.utils.CDCUtil;

public class OracleServer {
	private final static Logger logger = Logger.getLogger(OracleServer.class);
	
	private String user;

	private final Map<String, Thread> threads = new HashMap<>();
	
	private final Map<String, SchemaTask> schemaTasks = new HashMap<>();
	
	private OracleServer() {
		try {
			load(System.getProperty("user.dir") + "/conf/configs.xml");
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	@SuppressWarnings("unchecked")
	private void load(String filePath) throws Exception {
		SAXReader reader = new SAXReader();
		Document document = reader.read(new File(filePath));
		Element root = document.getRootElement().element("oracle");
		user = root.valueOf("cdcuser");
		String password = root.valueOf("password");
		String url = root.valueOf("url");
		String dba = root.valueOf("dba-account");
		String dbaPassword = root.valueOf("dba-password");

		ConnectionFactory.setUrl(url);
		ConnectionFactory.setUser(user);
		ConnectionFactory.setPassword(password);
		ConnectionFactory.setDba(dba);
		ConnectionFactory.setDbaPassword(dbaPassword);

		String driver = root.valueOf("driver");
		Class.forName(driver);

		Connection dbaConnection = ConnectionFactory.getDbaConnection();
		CDCUtil.initSystem(dbaConnection);

		tablespace = root.valueOf("tablespace");
		String datafile = root.valueOf("datafile");
		String fileSize = root.valueOf("file-size");
		String tempspace = root.valueOf("tempspace");
		String tempfile = root.valueOf("tempfile");
		String tempSize = root.valueOf("temp-size");
		changeSet = root.valueOf("change-set");
		
		if (!CDCUtil.userExist(dbaConnection, user)) {
			CDCUtil.createTableSpace(dbaConnection, tablespace, datafile, fileSize, fileSize);
			CDCUtil.createTempSpace(dbaConnection, tempspace, tempfile, tempSize, tempSize);
			CDCUtil.createUser(dbaConnection, user, password, tablespace, tempspace);
			CDCUtil.grantBasePrivilege(dbaConnection, user);
			CDCUtil.grantCDCPrivilege(dbaConnection, user);
		}

		grantPrivilage(dbaConnection, user);

		Connection connection = ConnectionFactory.getConnection();
		if (!CDCUtil.existChangeSet(connection, changeSet)) {
			CDCUtil.createChangeSet(connection, changeSet);
		}
		logger.info("Database setup over!");

		Iterator<Element> iterator = root.elementIterator("schema");
		while(iterator.hasNext()) {
			Element schema = iterator.next();
			String name = schema.attributeValue("name");
			String pwd = schema.attributeValue("password");

			SchemaModel schemaModel = new SchemaModel();
			schemaModel.setName(name);
			schemaModel.setUrl(url);
			schemaModel.setPassword(pwd);

			Iterator<Element> tables = schema.elementIterator();
			while(tables.hasNext()) {
				Element table = tables.next();
				String tableName = table.attributeValue("name");
				TableModel tableModel = new TableModel();
				tableModel.setLocalTable(tableName);
				tableModel.setSchema(schemaModel);
				tableModel.setColumns(CDCUtil.getTableColumns(connection, 
						name.toUpperCase(), tableName.toUpperCase()));
				Iterator<Element> fields = table.elementIterator();
				List<String> tableFields = new ArrayList<>();
				while(fields.hasNext()) {
					Element field = fields.next();
					String fieldName = field.getStringValue();
					tableFields.add(fieldName);
				}
				tableModel.setFields(tableFields);
				createCDC(connection, tableModel);
				schemaModel.getTables().put(tableName, tableModel);
			}
			SchemaTask schemaTask = new SchemaTask(schemaModel);
			Thread thread = new Thread(schemaTask, name);
			schemaTasks.put(name, schemaTask);
			thread.start();
			threads.put(name, thread);
		}
		Thread thread = new Thread(new CleanThread());
		thread.start();
		threads.put("clean-thread", thread);
		dbaConnection.close();
		connection.close();
	}
	
	private void grantPrivilage(Connection connection, String toUser) {
		StringBuilder sb = new StringBuilder();
		sb.append("grant")
		.append(" delete any table")
		.append(", select any table")
		.append(", update any table")
		.append(", insert any table")
		.append(" to ").append(toUser);
		try {
			CDCUtil.execute(connection, sb.toString());
		} catch (SQLException e) {
			logger.error(sb, e);
		}
	}
	
	private void createCDC(Connection conn, TableModel tableModel) {
		try {
			String fieldList = null;
			String schemaName = tableModel.getSchema().getName();
			String tableName = tableModel.getLocalTable();
			String changeTableName = schemaName + "_" + tableName;
			if (changeTableName.length() > 30) {
				changeTableName = schemaName + tableName.hashCode();
				changeTableName = changeTableName.replace("-", "_");
			}
			String subscrpName = changeTableName + "_SUB";
			if (subscrpName.length() > 30) {
				subscrpName = "SUB" + changeTableName.hashCode();
				subscrpName = subscrpName.replace("-", "_");
			}

			StringBuilder sb = new StringBuilder();

			Map<String, Column> columns = tableModel.getColumns();
			List<String> fields = tableModel.getFields();
			if (fields != null && fields.size() != 0) {
				for (Column column : columns.values()) {
					if (column.getDataTypeCode() == 2002) {
						continue;
					}
					if (column.getDataTypeCode() == Types.TIMESTAMP) {
						sb.append(column.getColumnName()).append(",");
						continue;
					}
					String columnName = column.getColumnName();
					if (fields.contains(columnName)) {
						sb.append(column.getColumnName()).append(" ")
						.append(column.getDataType()).append("(")
						.append(column.getLength()).append("), ");
					}
				}
			} else {
				for (Column column : columns.values()) {
					if (column.getDataTypeCode() == Types.STRUCT) {
						continue;
					}
					if (column.getDataTypeCode() == Types.TIMESTAMP) {
						sb.append(column.getColumnName()).append(" ")
						.append(column.getDataType()).append(",");
						continue;
					}
					sb.append(column.getColumnName()).append(" ")
					.append(column.getDataType()).append("(")
					.append(column.getLength()).append("), ");
				}
			}

			sb.deleteCharAt(sb.lastIndexOf(","));
			fieldList = sb.toString()
					.replaceAll("LOB\\(\\d*\\)", "LOB")
					.replaceAll("DATE\\(\\d*\\)", "DATE")
					.replaceAll("STRUCT\\(\\d*\\)", "STRUCT");

			if (!CDCUtil.existChangeTable(conn, changeTableName)) {
				CDCUtil.createChangeTable(conn, user, changeTableName, changeSet, 
						schemaName, tableName, fieldList, "both", tablespace);
				if (CDCUtil.existSubscription(conn, subscrpName)) {
					CDCUtil.dropSubscription(conn, subscrpName);
				}
				CDCUtil.createSubscription(conn, subscrpName, changeSet);
				CDCUtil.subscribe(conn, subscrpName, schemaName, tableName, subscrpName, fieldList);
				CDCUtil.activeSubscrpt(conn, subscrpName);
				return;
			}
			if (!CDCUtil.existSubscription(conn, subscrpName)) {
				CDCUtil.createSubscription(conn, subscrpName, changeSet);
			}
			if (!CDCUtil.existView(conn, subscrpName)) {
				CDCUtil.subscribe(conn, subscrpName, schemaName, tableName, subscrpName, fieldList);
				CDCUtil.activeSubscrpt(conn, subscrpName);
			}
		} catch (Exception e) {
			logger.error(tableModel, e);
		}
	}
	
	public void createCDC(TableModel tableModel) throws SQLException{
		Connection connection = ConnectionFactory.getConnection();
		createCDC(connection, tableModel);
		try {
			connection.close();
		} catch (Exception e) {
		}
	}
	
	public void dropCDC(TableModel tableModel) throws SQLException{
		Connection connection = ConnectionFactory.getConnection();
		String schemaName = tableModel.getSchema().getName();
		String tableName = tableModel.getLocalTable();
		String changeTableName = schemaName + "_" + tableName;
		if (changeTableName.length() > 30) {
			changeTableName = schemaName + tableName.hashCode();
			changeTableName = changeTableName.replace("-", "_");
		}
		String subscrpName = changeTableName + "_SUB";
		if (subscrpName.length() > 30) {
			subscrpName = "SUB" + changeTableName.hashCode();
			subscrpName = subscrpName.replace("-", "_");
		}
		
		CDCUtil.dropSubscription(connection, subscrpName);
		CDCUtil.dropChangeTable(connection, user, changeTableName);
		try {
			connection.close();
		} catch (Exception e) {
		}
	}
	
	public TableModel civilizeTable(SchemaModel schemaModel, String tableName) 
			throws SQLException{
		Connection connection = ConnectionFactory.getConnection();
		TableModel tableModel = new TableModel();
		tableModel.setLocalTable(tableName);
		tableModel.setSchema(schemaModel);
		tableModel.setColumns(CDCUtil.getTableColumns(connection, 
				schemaModel.getName().toUpperCase(), tableName.toUpperCase()));
		List<String> tableFields = new ArrayList<>();
		tableModel.setFields(tableFields);
		createCDC(connection, tableModel);
		try {
			connection.close();
		} catch (Exception e) {
		}
		return tableModel;
	}

	public Map<String, Thread> getThreads() {
		return threads;
	}

	public Map<String, SchemaTask> getSchemaTasks() {
		return schemaTasks;
	}

	public class SchemaTask extends AbstractTask {
		private final SchemaModel schemaModel;
		
		public SchemaTask(SchemaModel schemaModel) {
			this.schemaModel = schemaModel;
		}
		
		public SchemaModel getSchemaModel() {
			return schemaModel;
		}

		@Override
		public void run() {
			while(running.get()){
				refresh();
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		public synchronized void refresh() {
			Collection<TableModel> tables = schemaModel.getTables().values();
			for (TableModel tableModel : tables) {
				try {
					Thread thread = null;
					String threadName = schemaModel.getName() + "." +tableModel.getLocalTable();
					Task task = tasks.get(threadName);
					boolean b = false;
					if (task == null || (b = task.isRunning())) {
						if (b) {
							if (threads.containsKey(threadName) && threads.get(threadName).isAlive()) {
								continue;
							} else {
								logger.warn("restart thread : " + threadName);
							}
						}
						else 
							logger.info("start thread " + threadName);
						task = new TableTask(tableModel);
						thread = new Thread(task, threadName);
						tasks.put(threadName, task);
						thread.start();
						threads.put(threadName, thread);
					} else {
						//the thread has been stopped normally
					}
				} catch (Exception e) {
					logger.error(tableModel, e);
				}
			}
		}
	}

	public class TableTask extends AbstractTask {
		private final TableModel tableModel;
		private Extractor extractor;

		public TableTask(TableModel tableModel) {
			this.tableModel = tableModel;
		}

		@Override
		public void run() {
			String schemaName = tableModel.getSchema().getName();
			String tableName = tableModel.getLocalTable();
			Connection connection = null;
			try {
				connection = ConnectionFactory.getConnection();
				Map<String, Column> columns = CDCUtil.getTableColumns(connection, schemaName, tableName);
				extractor = new Extractor();
				extractor.setConnection(connection);
				extractor.setColumns(columns);
				extractor.setTableName(tableName);
				extractor.setSchema(schemaName);
				extractor.startup();
			} catch (SQLException e) {
				logger.error(tableModel, e);
			} finally {
				stop();
			}
		}
		
		@Override
		public void stop() {
			super.stop();
			if(extractor != null)
				extractor.stop();
		}

		public TableModel getTableModel() {
			return tableModel;
		}
		
	}
	
	class CleanThread extends AbstractTask {
		@Override
		public void run() {
			while(running.get()){
				Connection connection = null;
				try {
					connection = ConnectionFactory.getConnection();
					CDCUtil.executePLSQL(connection, "dbms_cdc_publish.purge;");
					Thread.sleep(60 * 1000 * 10);
				} catch (SQLException e) {
					logger.error(this, e);
				} catch (InterruptedException e) {
					logger.error(e);
				} finally {
					if (connection != null) {
						try {
							connection.close();
						} catch (SQLException e) {
							logger.error(this, e);
						}
					}
				}	
			}
		}
		
	}

	private final static OracleServer server = new OracleServer();
	private String changeSet;
	private String tablespace;
	public static OracleServer newInstance() {
		return server;
	} 

	public static void main(String[] args) {}

}
