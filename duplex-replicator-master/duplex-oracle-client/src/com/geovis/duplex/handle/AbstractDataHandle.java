package com.geovis.duplex.handle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import org.apache.log4j.Logger;

import com.geovis.duplex.activemq.receive.MessagePull;
import com.geovis.duplex.driver.ConnectionFactory;
import com.geovis.duplex.model.Carrier;
import com.geovis.duplex.model.Column;
import com.geovis.duplex.model.TableModel;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;
import oracle.sql.TypeDescriptor;

public abstract class AbstractDataHandle implements DatabaseHandle {
	private static final Logger logger = Logger.getLogger(AbstractDataHandle.class);

	protected Connection connection;
	protected PreparedStatement insertStmt;
	protected PreparedStatement updateStmt;
	protected PreparedStatement deleteStmt;
	protected MessagePull puller;
	protected TableModel tableModel;

	protected List<Carrier> list = new ArrayList<Carrier>();

	private Map<String, PKTableHandle> pKTableHandles = new HashMap<String, PKTableHandle>();

	protected String[] names;
	protected int[] types;

	protected Clob clob;


	{
		try {
			connection = ConnectionFactory.getConnection();
		} catch (SQLException e) {
			logger.error(e, e.getCause());;
		}
	}

	@Override
	public void handle(Message message) throws SQLException {
		try {
			handle((Carrier) ((ObjectMessage) message).getObject());
		} catch (JMSException e) {
			logger.error(tableModel, e);
		}

		try {
			execute();
		} catch (Exception e) {
			logger.error(tableModel, e);
		}

	}

	public void handle(Carrier carrier) {
		if (carrier != null) {
			for (String columnName : pKTableHandles.keySet()) {
				PKTableHandle pkTableHandle = pKTableHandles.get(columnName);
				if (carrier.getDependencies() != null) {
					pkTableHandle.handle(carrier.getDependencies().get(columnName));
					carrier.getDependencies().remove(columnName);
				}
			}
			try {
				if (carrier.getOpttype().trim().equals("I")) {
					insert(carrier);
				} else if (carrier.getOpttype().trim().equals("D")) {
					delete(carrier);
				} else if (carrier.getOpttype().trim().equals("UN")) {
					update(carrier);
				} else {
					logger.info("handle message missed, do insert by default!");
					insert(carrier);
				}
			} catch (SQLException e) {
				if (e.getErrorCode() == 17410) {
					puller.rollback();
					puller.stop();
				}
			}

		} else {
			logger.error("The primary table record is null!");
			logger.error(this.getClass().getName());
		}
	}

	@Override
	public void execute() {
		flush();
	}

	protected void fill() throws SQLException {
		for (DatabaseHandle pkHandle : pKTableHandles.values()) {
			pkHandle.execute();
		}
		for (Carrier carrier : list) {
			if (carrier.getOpttype().trim().equals("I")) {
				insert(carrier);
			} else if (carrier.getOpttype().trim().equals("D")) {
				delete(carrier);
			} else if (carrier.getOpttype().trim().equals("UN")) {
				update(carrier);
			} else {
				logger.info("handle message missed, do insert by default!");
				insert(carrier);
			}
		}
	}

	protected void flush() {
		try {
			connection.commit();
			puller.commit();
		} catch (SQLException e) {
			if (e.getErrorCode() != 1) {
				try {
					puller.rollback();
					connection.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				logger.error(tableModel, e);;
			}
		}
	}

	protected void execUpdate() {
		try {
			updateStmt.executeBatch();
		} catch (SQLException e) {
			logger.error(tableModel, e);
		}
	}

	protected void execDelete() {
		try {
			deleteStmt.executeBatch();
		} catch (SQLException e) {
			logger.error(tableModel, e);
		}
	}

	protected void execInsert() {
		try {
			insertStmt.executeBatch();
		} catch (SQLException e) {
			if (e.getErrorCode() != 1) {
				logger.error(tableModel, e);
			}
		} 
	}

	protected abstract void update(Carrier carrier) throws SQLException;

	protected abstract void delete(Carrier carrier) throws SQLException;

	protected byte[] parseLob(StringBuffer fieldvalue) throws IOException {
		ByteArrayInputStream bais =
				new ByteArrayInputStream(Base64.decode(fieldvalue.toString()));
		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
		byte[] temp = new byte[1024];
		int size = 0;
		try {
			while ((size = bais.read(temp)) != -1) {
				out.write(temp, 0, size);
			}
		} catch (IOException e) {
			logger.error(tableModel, e);
		}
		byte[] content = out.toByteArray();
		bais.close();
		out.close();
		return content;
	}

	@Override
	public void clear() {
		for (DatabaseHandle databaseHandle : pKTableHandles.values()) {
			databaseHandle.clear();
		}
		list.clear();
		list = new ArrayList<Carrier>();
	}

	@Override
	public void close() {
		try {
			insertStmt.close();
			updateStmt.close();
			deleteStmt.close();
			for (DatabaseHandle databaseHandle : pKTableHandles.values()) {
				databaseHandle.close();
			}
		} catch (SQLException e) {
			logger.error(tableModel, e);
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				logger.error(tableModel, e);
			}
		}

	}

	public void setConnection(Connection conn) {
		this.connection = conn;
	}

	protected Map<String, StructDescriptor> descriptors = new HashMap<String, StructDescriptor>();

	@Override
	public void setup() throws SQLException {
		for (Column column : tableModel.getColumns().values()) {
			if (column.getPkColumnName() != null && 
					!"".equals(column.getPkColumnName().trim())) {
				PKTableHandle pkHandle = new PKTableHandle(this.connection, column);
				pkHandle.setup();
				pKTableHandles.put(column.getFkColumnName(), pkHandle);
			}
			if (column.getDataTypeCode() == 2002) {
				StructDescriptor descriptor = 
						StructDescriptor.createDescriptor(column.getDataType(), connection);
				descriptors.put(column.getColumnName(), descriptor);
			}
		}
	}

	protected void insert(Carrier carrier) throws SQLException {
		Map<String, StringBuffer> fields = carrier.getBody();
		a : while(true){
			try {
				for(int i=0; i < names.length; i++){
					String name = names[i];

					StringBuffer fieldvalue = fields.get(name);

					int parameterIndex = i + 1;
					if (types[i] == TypeDescriptor.TYPECODE_JDBC_STRUCT) {
						insertStmt.setObject(parameterIndex, parseStruct(name, fieldvalue));
						continue;
					}

					if (fields.containsKey(name) && !"".equals(fieldvalue.toString())) {
						if (types[i] == Types.BLOB || types[i] == Types.LONGVARBINARY 
								|| types[i] == TypeDescriptor.TYPECODE_BLOB
								) {
							//						byte[] content = parseLob(fieldvalue);
							insertStmt.setBytes(parameterIndex, Base64.decode(fieldvalue.toString()));
						}
						else if (types[i] == Types.CLOB || types[i] == TypeDescriptor.TYPECODE_CLOB) {
							long size = clob.length();
							try {
								if(size > 0)
									clob.truncate(size);
							} catch (Exception e) {
							}
							clob.setString(1, fieldvalue.toString());
							insertStmt.setClob(parameterIndex, clob);
						}
						else if  (types[i] == Types.TIME || types[i] == Types.DATE
								|| types[i] == Types.TIMESTAMP) {
							insertStmt.setTimestamp(parameterIndex, 
									new Timestamp(Long.parseLong(fieldvalue.toString())));
						}
						else {
							insertStmt.setObject(parameterIndex, fieldvalue.toString());
						}
					} else {
						insertStmt.setNull(parameterIndex, types[i]);
					}

				}
				insertStmt.execute();
				return;
			} catch (SQLException e) {
				switch (e.getErrorCode()) {
				case 1:
					return;
				case 2291:
					continue a;
				case 17410:
					throw e;
				default:
					logger.error(carrier, e);
					throw e;
				}
			}
		}
	}

	protected STRUCT parseStruct(String name, StringBuffer fieldvalue) {
		try {
			if (fieldvalue == null) {
				return new STRUCT(descriptors.get(name), new byte[0], connection);
			}
			return new STRUCT(descriptors.get(name), 
					Base64.decode(fieldvalue.toString()), connection);
		} catch (SQLException e) {
			logger.error(tableModel, e);
		}
		return null;
	}

	public int getQueueSize(){
		return list.size();
	}

	@SuppressWarnings("unused")
	private int getTypeNumber(String type) {
		if (type.equals("VARCHAR")) {
			return Types.VARCHAR;
		} else  if (type.equals("BOOLEAN")) {
			return Types.BOOLEAN;
		} else  if (type.equals("BIGINT")) {
			return Types.BIGINT;
		} else  if (type.equals("DOUBLE")) {
			return Types.DOUBLE;
		} else  if (type.equals("INTEGER")) {
			return Types.INTEGER;
		} else  if (type.equals("NUMERIC")) {
			return Types.NUMERIC;
		} else  if (type.equals("REAL")) {
			return Types.REAL;
		} else  if (type.equals("SMALLINT")) {
			return Types.SMALLINT;
		} else  if (type.equals("BLOB")) {
			return Types.BLOB;
		} else  if (type.equals("NVARCHAR")) {
			return Types.NVARCHAR;
		} else  if (type.equals("ARRAY")) {
			return Types.ARRAY;
		} else  if (type.equals("DATALINK")) {
			return Types.DATALINK;
		} else  if (type.equals("DATE")) {
			return Types.DATE;
		} else  if (type.equals("DISTINCT")) {
			return Types.DISTINCT;
		} else  if (type.equals("JAVA_OBJECT")) {
			return Types.JAVA_OBJECT;
		} else  if (type.equals("LONGNVARCHAR")) {
			return Types.LONGNVARCHAR;
		} else  if (type.equals("NCHAR")) {
			return Types.NCHAR;
		} else  if (type.equals("NCLOB")) {
			return Types.NCLOB;
		} else  if (type.equals("NULL")) {
			return Types.NULL;
		} else  if (type.equals("OTHER")) {
			return Types.OTHER;
		} else  if (type.equals("REF")) {
			return Types.REF;
		} else  if (type.equals("ROWID")) {
			return Types.ROWID;
		} else  if (type.equals("SQLXML")) {
			return Types.SQLXML;
		} else  if (type.equals("STRUCT")) {
			return Types.STRUCT;
		} else  if (type.equals("TIME")) {
			return Types.TIME;
		} else  if (type.equals("TIMESTAMP")) {
			return Types.TIMESTAMP;
		} else  if (type.equals("TINYINT")) {
			return Types.TINYINT;
		} else  if (type.equals("LONGVARBINARY")) {
			return Types.LONGVARBINARY;
		} 
		return Types.VARCHAR;
	}

	public void setPuller(MessagePull puller) {
		this.puller = puller;
	}

}
