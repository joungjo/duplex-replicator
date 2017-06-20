package com.geovis.duplex.extract;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.geovis.duplex.activemq.push.MessagePush;
import com.geovis.duplex.activemq.push.SyncroPush;
import com.geovis.duplex.driver.ConnectionFactory;
import com.geovis.duplex.model.Carrier;
import com.geovis.duplex.model.Column;
import com.geovis.duplex.utils.CDCUtil;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import oracle.jdbc.OracleResultSet;
import oracle.sql.CLOB;
import oracle.sql.OPAQUE;
import oracle.sql.STRUCT;
import oracle.xdb.XMLType;

/**
 * 抽取数据库数据，调用MessagePush接口，通过jmx发送至中间件上
 * @author Jangzo
 *
 */
public class Extractor implements DataExtract {
	private final static Logger logger = Logger.getLogger(Extractor.class);

	protected Connection connection;
	protected String tableName;
	protected String schema;
	protected PreparedStatement stmt;
	protected ResultSet rs;
	protected Map<Integer, Integer> columnTypes;
	protected MessagePush push;
	protected Map<String, FKDataExtractor> sourceExtractors = 
			new HashMap<String, FKDataExtractor>();
	protected Map<String, Column> columns;
	protected final AtomicBoolean running = new AtomicBoolean(false);
	private List<Column> keys = new ArrayList<>();
	private String subscrpName;
	/* 
	 * should be deleted
	 */
	public Extractor() {}

	protected Extractor(Connection connection, MessagePush push) {
		this.push = push;
		this.connection = connection;
	}

	public Extractor(Connection connection, MessagePush push, Map<String, Column> columns) {
		this.push = push;
		this.connection = connection;
		this.columns = columns;
	}

	public void startup(){
		if(!this.running.compareAndSet(false, true)) {
			return;
		}

		push = new SyncroPush();
		push.setTopic(null, schema + "." + tableName);

		for (Column column : columns.values()) {
			String fkColumnName = column.getFkColumnName();
			if (fkColumnName != null && !fkColumnName.trim().equals("")) {
				FKDataExtractor sourceExtractor = 
						new FKDataExtractor(connection, column);
				sourceExtractor.startup();
				sourceExtractors.put(fkColumnName, sourceExtractor);
			}
		}
		initUDTs(columns);
		try {
			String changeTableName = schema + "_" + tableName;
			if (changeTableName.length() > 30) {
				changeTableName = schema + tableName.hashCode();
				changeTableName = changeTableName.replace("-", "_");
			}
			prepare(keySQL(changeTableName));
			while(running.get()){
				executeQuery();
				clear();
			}
			close();
		} catch (SQLException e) {
			logger.error(schema + "." + tableName, e);
		} finally {
			close();
		}
	}

	protected void initUDTs(Map<String, Column> columns) {
		List<Column> udtColumns = new ArrayList<>();
		for (Column column : columns.values()) {
			if (column.getDataTypeCode() == 2002) {
				udtColumns.add(column);
			}
			if (column.isKey()) {
				keys.add(column);
			}
		}
		if (udtColumns.size() > 0) {
			udts = new UDTExtractor();
			udts.init(schema, tableName, udtColumns);
		}
	}

	/**
	 * 初始化有主键查询语句
	 * @return 执行查询的sql语句
	 */
	private String keySQL(String table){
		subscrpName = table + "_SUB";
		if (subscrpName.length() > 30) {
			subscrpName = "SUB" + table.hashCode();
			subscrpName = subscrpName.replace("-", "_");
		}
		return "select * from " + subscrpName
				+ " where USERNAME$ != '"+ ConnectionFactory.getUser() +"' and OPERATION$ != 'UU' "
				+ "and OPERATION$ != 'UO'";
	} 

	@Override
	public void stop() {
		this.running.compareAndSet(true, false);
	}

	public void close() {
		try {
			rs.close();
			stmt.close();
			for (FKDataExtractor s : sourceExtractors.values()) {
				s.stop();
			}
			if (udts != null) {
				udts.close();
			}
		} catch (SQLException e) {
			logger.error(this, e);
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				logger.error(this, e);
			}
		}
	}

	protected int increase = 0;
	protected long time = 0;
	@Override
	public void clear() {
		if(increase != 0) {
			StringBuilder sb = new StringBuilder();
			sb.append(schema).append(".").append(tableName)
			.append(":").append(increase).append(" ")
			.append(String.format("%.2f", 1000 * increase / (1.0 * System.currentTimeMillis() - time)))
			.append("/s");
			logger.info(sb);
		}
	}

	@Override
	public void executeQuery() throws SQLException {
		extendSub();
		time = System.currentTimeMillis();
		increase = 0;
		rs = stmt.executeQuery();
		ResultSetMetaData resultMetaData = rs.getMetaData();
		int fieldCount = resultMetaData.getColumnCount();
		if (rs.next()) {
			do {
				Carrier carrier = fetchVaues(resultMetaData, fieldCount);
				push.push(carrier);
				increase++;
			} while (rs.next());

			try {
				rs.close();
			} catch (SQLException e) {
				logger.error(this, e);
			}
		} else {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		clearSub();
	}

	protected Carrier fetchVaues(ResultSetMetaData resultMetaData, int fieldCount) throws SQLException {
		HashMap<String, StringBuffer> hm = new HashMap<String, StringBuffer>();
		//遍历字段
		for (int i = 1; i <= fieldCount; i++) {
			StringBuffer fieldValue = new StringBuffer();
			if (rs.getObject(i) != null) {
				switch (columnTypes.get(i)) {
				case 0:
					fieldValue.append(rs.getString(i));
					break;

				case 1:
					Clob clob = rs.getClob(i);
					fieldValue.append(clob.getSubString(1, (int)clob.length()));
					break;

				case 2:
					OracleResultSet ors = (OracleResultSet) rs;
					OPAQUE op = ors.getOPAQUE(i);
					if (null != op) {
						XMLType xmlType = XMLType.createXML(op);
						fieldValue.append(xmlType.getStringVal());
					} 
					break;

				case 3:
					try {
						Blob blob = rs.getBlob(i);
						if (blob != null) {
							fieldValue.append(Base64.encode(blob.getBytes(1, (int)blob.length())));
						}
					} catch (Exception e) {
						logger.error(this, e);
					}
					break;

				case 4:
					fieldValue.append(rs.getTimestamp(i).getTime());
					break;

				case 5:
					fieldValue = parseStruct(rs.getObject(i));
					break;

				default:
					break;
				}
			}
			hm.put(resultMetaData.getColumnName(i), fieldValue);
		}
		Carrier carrier = cover(hm);
		fKeysAndUdts(carrier);
		return carrier;
	}

	protected void fKeysAndUdts(Carrier carrier) throws SQLException {
		if (!rs.getString("OPERATION$").trim().equals("D")) {
			//insert
			if (rs.getString("OPERATION$").trim().equalsIgnoreCase("I")) {
				//iterate foreign keys
				for (String fkey : sourceExtractors.keySet()) {
					sourceExtractors.get(fkey).executeQuery(rs.getString(fkey), carrier);
				}
			}
			//insert and update
			if (udts != null) {
				for (int j = 0; j < udts.pk.length; j++) {
					udts.setParameter(j + 1, rs.getString(udts.pk[j]));
				}
				Map<String, StringBuffer> udtValues = udts.getUDTValues();
				if (udtValues != null) {
					HashMap<String, StringBuffer> hm = carrier.getBody();
					for (String key : udtValues.keySet()) {
						hm.put(key, udtValues.get(key));
					}
				}
			}
		}
	}

	protected void clearSub() throws SQLException {
		CDCUtil.purgeWindow(connection, subscrpName);
	}
	protected void extendSub() throws SQLException {
		CDCUtil.extendWindow(connection, subscrpName);
	}

	@SuppressWarnings("unused")
	private String parseBlob(Blob blob) throws SQLException, IOException {
		InputStream inputStream = blob.getBinaryStream();
		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
		byte[] temp = new byte[1024];
		int size = 0;
		while ((size = inputStream.read(temp)) != -1) {
			out.write(temp, 0, size);
		}
		inputStream.close();
		byte[] content = out.toByteArray();
		out.close();
		return Base64.encode(content);
	}

	/**
	 * oracle user-defined type
	 * @param value
	 * @return
	 */
	protected StringBuffer parseStruct(Object value) {
		if (value == null) return null;
		STRUCT struct = (STRUCT) value;
		byte[] bytes = struct.getBytes();
		return new StringBuffer(Base64.encode(bytes));
	}

	protected  Carrier cover(HashMap<String, StringBuffer> hm) {
		Carrier carrier = new Carrier();
		carrier.setTablename(tableName);
		carrier.setOpttype(hm.get("OPERATION$").toString());
		carrier.setBody(hm);
		return carrier;
	}

	protected void initColumnTypes(ResultSetMetaData resultMetaData)
			throws SQLException {
		columnTypes = new HashMap<Integer, Integer>();

		int fieldCount = resultMetaData.getColumnCount();

		for (int i = 1; i <= fieldCount; i++) {
			int type = resultMetaData.getColumnType(i);
			//
			if (type == oracle.sql.TypeDescriptor.TYPECODE_CLOB
					|| type == Types.CLOB
					) {
				columnTypes.put(i, 1);
			} 
			//
			else if (type == 2007) {
				columnTypes.put(i, 2);
			}
			//blob
			else if (type == Types.BLOB || type == Types.LONGVARBINARY) {
				columnTypes.put(i, 3);
			} 
			//Time,Date,Timestamp
			else if (type == Types.TIME || type == Types.TIMESTAMP
					|| type == Types.TIMESTAMP) {
				columnTypes.put(i, 4);
			}
			/*//struct
			else if (type == oracle.sql.TypeDescriptor.TYPECODE_JDBC_STRUCT){
				columnTypes.put(i, 5);
			}
			//
			 */
			else {
				columnTypes.put(i, 0);
			}

		}
	}

	@Override
	public void prepare(String sql) throws SQLException {
		int result_count = 1000;
		stmt = connection.prepareStatement(sql, 
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(result_count);
		String initSql = sql.substring(0, sql.lastIndexOf("where")) + "where 0 > 1";
		parseColums(initSql);
	}

	protected void parseColums(String sql) throws SQLException {
		Statement statement = connection.createStatement();
		ResultSet rSet = statement.executeQuery(sql);
		ResultSetMetaData metaData = rSet.getMetaData();
		initColumnTypes(metaData);
		statement.close();
		rSet.close();
	}

	/**
	 * 对Clob数据进行处理
	 * 将二进制流数据转换成字符串
	 */
	protected String convertClobToString(CLOB clob) {
		String reString = "";
		try {
			Reader is = clob.getCharacterStream();// 得到流
			BufferedReader br = new BufferedReader(is);
			String s = br.readLine();
			StringBuffer sb = new StringBuffer();
			while (s != null) {
				sb.append(s);
				sb.append("\n");
				s = br.readLine();
			}
			reString = sb.toString().trim();
		} catch (Exception e) {
			logger.error(this, e);;
		}
		return reString;
	}

	/**
	 * 用户自定义类型。处理方式暂定为从原表查询所有用户自定义类型字段的值
	 */
	protected UDTExtractor udts;
	/**
	 * 该类查询原表中的用户自定义数据类型字段的值，主要是oracle的STRUCT类型。
	 * @author Jangzo
	 * 2016-07-06
	 */
	class UDTExtractor{
		private PreparedStatement pstmt;
		private String[] columnNames;
		public String[] pk;

		public Map<String, StringBuffer> getUDTValues(){
			try {
				ResultSet results = pstmt.executeQuery();
				if (results.next()) {
					Map<String, StringBuffer> map = new HashMap<String, StringBuffer>();
					for (int i = 0; i < columnNames.length; i++) {
						map.put(columnNames[i], parseStruct(results.getObject(columnNames[i])));
					}
					results.close();
					return map;
				}
			} catch (SQLException e) {
				logger.error(this, e);
			} 
			return null;
		}

		public void setParameter(int i, String value){
			try {
				pstmt.setString(i, value);
			} catch (SQLException e) {
				logger.error(this, e);
			}
		}

		public void init(String schema, String tableName, 
				List<Column> udtColums){
			columnNames = new String[udtColums.size()];
			StringBuilder sb = new StringBuilder("select ");

			int num = 0;
			for (Column column : udtColums) {
				columnNames[num] = column.getColumnName();
				sb.append(columnNames[num] + ",");
				++num;
			}
			sb.deleteCharAt(sb.lastIndexOf(","));
			sb.append(" from ");
			sb.append(schema);
			sb.append(".");
			sb.append(tableName);
			sb.append(" where 1=1");
			pk = new String[keys.size()];
			for (int i = 0; i < pk.length; i++) {
				pk[i] = keys.get(i).getColumnName();
				sb.append(" and ");
				sb.append(pk[i]);
				sb.append("=?");
			}

			try {
				pstmt = connection.prepareStatement(sb.toString());
			} catch (SQLException e) {
				logger.error(this, e);
			}
		}

		public void close(){
			try {
				pstmt.close();
			} catch (SQLException e) {
				logger.error(this, e);
			}
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public MessagePush getPush() {
		return push;
	}

	public void setPush(MessagePush push) {
		this.push = push;
	}

	public Map<String, Column> getColumns() {
		return columns;
	}

	public void setColumns(Map<String, Column> columns) {
		this.columns = columns;
	}

	public UDTExtractor getUdts() {
		return udts;
	}

	public void setUdts(UDTExtractor udts) {
		this.udts = udts;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}
	
}
