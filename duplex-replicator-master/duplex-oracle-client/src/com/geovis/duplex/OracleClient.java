package com.geovis.duplex;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.geovis.duplex.activemq.receive.SynchronousReceive;
import com.geovis.duplex.driver.ConnectionFactory;
import com.geovis.duplex.model.NodeModel;
import com.geovis.duplex.model.SchemaModel;
import com.geovis.duplex.model.TableModel;
import com.geovis.duplex.utils.CDCUtil;

public class OracleClient {
	private final static Logger logger = Logger.getLogger(OracleClient.class);

	private String user;
	private String password;
	private String url;
	private String dba;
	private String dbaPassword;

	private HashMap<String, Thread> threads = new HashMap<>();

	private OracleClient() {
		try {
			load(System.getProperty("user.dir") + "/conf/configs.xml");
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	@SuppressWarnings("unchecked")
	public void load(String filePath) throws Exception {
		Document document = new SAXReader().read(new File(filePath));
		Element root = document.getRootElement().element("oracle");
		user = root.valueOf("cdcuser");
		password = root.valueOf("password");
		url = root.valueOf("url");
		dba = root.valueOf("dba-account");
		dbaPassword = root.valueOf("dba-password");

		ConnectionFactory.setUrl(url);
		ConnectionFactory.setUser(user);
		ConnectionFactory.setPassword(password);
		ConnectionFactory.setDba(dba);
		ConnectionFactory.setDbaPassword(dbaPassword);

		String driver = root.valueOf("driver");
		Class.forName(driver);

		Connection dbaConnection = ConnectionFactory.getDbaConnection();
		grantPrivilage(dbaConnection, user);
		CDCUtil.initSystem(dbaConnection);

		String tablespace = root.valueOf("tablespace");
		String datafile = root.valueOf("datafile");
		String fileSize = root.valueOf("file-size");
		String tempspace = root.valueOf("tempspace");
		String tempfile = root.valueOf("tempfile");
		String tempSize = root.valueOf("temp-size");
		if (!CDCUtil.userExist(dbaConnection, user)) {
			CDCUtil.createTableSpace(dbaConnection, tablespace, datafile, fileSize, fileSize);
			CDCUtil.createTempSpace(dbaConnection, tempspace, tempfile, tempSize, tempSize);
			CDCUtil.createUser(dbaConnection, user, password, tablespace, tempspace);
			CDCUtil.grantBasePrivilege(dbaConnection, user);
			CDCUtil.grantCDCPrivilege(dbaConnection, user);
		}

		Connection connection = ConnectionFactory.getConnection();

		Iterator<Element> nodes = root.elementIterator("node");

		while(nodes.hasNext()) {
			NodeModel nodeModel = new NodeModel();
			Element node = nodes.next();
			nodeModel.setAddress(node.attributeValue("ip"));
			nodeModel.setPort(Integer.parseInt(node.attributeValue("port")));
			Iterator<Element> iterator = node.elementIterator("schema");
			List<SchemaModel> schemas = new ArrayList<>();
			while(iterator.hasNext()) {
				Element schema = iterator.next();
				SchemaModel schemaModel = new SchemaModel();
				String remoteSchema = schema.attributeValue("remote");
				schemaModel.setRemote(remoteSchema);
				schemaModel.setNode(nodeModel);
				Iterator<Element> tables = schema.elementIterator();
				while(tables.hasNext()) {
					TableModel tableModel = new TableModel();
					Element table = tables.next();

					String remoteTable = table.attributeValue("remote");
					String localTable = table.attributeValue("localTable");
					tableModel.setRemoteTable(remoteTable);
					if (localTable == null || localTable.trim().equals("")) {
						tableModel.setLocalTable(remoteTable);
						localTable = remoteTable;
					} 
					tableModel.setLocalTable(localTable);
					String localSchema = table.attributeValue("localSchema");
					if (localSchema == null || localSchema.trim().equals("")) {
						localSchema = remoteSchema;
					}
					tableModel.setLocalSchema(localSchema);

					tableModel.setSchema(schemaModel);
					tableModel.setUrl(url);
					tableModel.setColumns(CDCUtil.getTableColumns(connection, 
							localSchema.toUpperCase(), localTable.toUpperCase()));
					Iterator<Element> fields = table.elementIterator();
					List<String> tableFields = new ArrayList<>();
					while(fields.hasNext()) {
						Element field = fields.next();
						String fieldName = field.getStringValue();
						tableFields.add(fieldName);
					}
					tableModel.setFields(tableFields);
					schemaModel.getTables().put(localTable, tableModel);
				}
				schemas.add(schemaModel);
			}
			new Thread(new SchemaThread(schemas)).start();
			nodeModel.setSchemaModels(schemas);
		}

		dbaConnection.close();
		connection.close();
	}

	class SchemaThread implements Runnable {
		private List<SchemaModel> schemas;

		public SchemaThread(List<SchemaModel> schemas) {
			this.schemas = schemas;
		}

		@Override
		public void run() {
			while(true) {
				try {
					for (SchemaModel schemaModel : schemas) {
						String remoteSchema = schemaModel.getRemote();
						Collection<TableModel> tables = schemaModel.getTables().values();
						for (TableModel tableModel : tables) {
							String threadName = remoteSchema + "." + tableModel.getRemoteTable();
							if (!threads.containsKey(threadName) || !threads.get(threadName).isAlive()) {
								Thread thread = new Thread(new ReceiveThread(tableModel));
								threads.put(threadName, thread);
								thread.start();
							}
						}
					}
					Thread.sleep(60000);
				} catch (Exception e) {
					logger.error(e, e.getCause());
				}
			}
		}
	}

	class ReceiveThread implements Runnable {
		private final TableModel tableModel;

		public ReceiveThread(TableModel tableModel) {
			this.tableModel = tableModel;
		}

		@Override
		public void run() {
			new SynchronousReceive(tableModel).startup();
		}

	}

	private void grantPrivilage(Connection connection, String toUser) {
		StringBuilder sb = new StringBuilder();
		try {
			sb.append("grant")
			.append(" delete any table")
			.append(", select any table")
			.append(", update any table")
			.append(", insert any table")
			.append(" to ").append(toUser);
			CDCUtil.execute(connection, sb.toString());
		} catch (SQLException e) {
			logger.error(sb);
			logger.error(e, e.getCause());
		}
	}

	private final static OracleClient CLIENT = new OracleClient();
	public static OracleClient newInstance() {
		return CLIENT;
	} 

	public static void main(String[] args) {}
}
