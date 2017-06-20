package com.geovis.duplex.handle;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.geovis.duplex.activemq.receive.SynchronousReceive;
import com.geovis.duplex.model.Carrier;
import com.geovis.duplex.model.Column;
import com.geovis.duplex.model.TableModel;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import oracle.sql.TypeDescriptor;

public class HandleByKey extends AbstractDataHandle {
	private static final Logger logger = Logger.getLogger(HandleByKey.class);
	
	private List<Column> keys = new ArrayList<>();
	private int length = 0;

	public HandleByKey() { }

	public HandleByKey(TableModel tableModel) throws SQLException {
		this.tableModel = tableModel;
	}

	public HandleByKey(TableModel tableModel, SynchronousReceive pull) 
			throws SQLException {
		this.tableModel = tableModel;
		this.puller = pull;
	}

	@Override
	protected void update(Carrier carrier) {

		Map<String, StringBuffer> fields = carrier.getBody();

		try {
			for(int i=0; i < length; i++){
				String name = names[i];

				StringBuffer fieldvalue = fields.get(name);

				int paramIndex = i + 1;
				if (types[i] == TypeDescriptor.TYPECODE_JDBC_STRUCT) {
					updateStmt.setObject(paramIndex, parseStruct(name, fieldvalue));
					continue;
				}

				if (fields.containsKey(name) && !"".equals(fieldvalue.toString())) {
					if (types[i] == Types.BLOB || types[i] == Types.LONGVARBINARY 
							|| types[i] == TypeDescriptor.TYPECODE_BLOB
							) {
						//						byte[] content = parseLob(fieldvalue);
						updateStmt.setBytes(paramIndex, Base64.decode(fieldvalue.toString()));
					}
					else if (types[i] == Types.CLOB || types[i] == TypeDescriptor.TYPECODE_CLOB) {
//						updateStmt.setBytes(paramIndex, Base64.decode(fieldvalue.toString()));
						long size = clob.length();
						try {
							if(size > 0)
								clob.truncate(size);
						} catch (Exception e) {
						}
						clob.setString(1, fieldvalue.toString());
						updateStmt.setClob(paramIndex, clob);
					}
					else if  (types[i] == Types.TIME || types[i] == Types.TIMESTAMP
							|| types[i] == Types.TIMESTAMP) {
						updateStmt.setTimestamp(paramIndex, 
								new Timestamp(Long.parseLong(fieldvalue.toString())));
					} else if (types[i] == TypeDescriptor.TYPECODE_JDBC_STRUCT) {
						updateStmt.setObject(paramIndex, parseStruct(name, fieldvalue));
					} 
					else {
						updateStmt.setObject(paramIndex, fieldvalue.toString());
					}
				} else {
					updateStmt.setNull(paramIndex, types[i]);
				}

			}

			for (int j = 0, size = keys.size(); j < size; j++) {
				updateStmt.setObject(length + j + 1, 
						fields.get(keys.get(j).getColumnName()).toString());
			}

			updateStmt.execute();
		} catch (NumberFormatException e) {
			logger.error(tableModel, e);
		} catch (SQLException e) {
			logger.error(tableModel, e);
		}

	}

	@Override
	protected void delete(Carrier carrier) {
		Map<String, StringBuffer> fields = carrier.getBody();
		try {
			for (int j = 0, size = keys.size(); j < size; j++) {
				deleteStmt.setObject(j + 1, fields.get(keys.get(j).getColumnName()).toString());
			}
			deleteStmt.execute();
		} catch (SQLException e) {
			logger.error(tableModel, e);
		}
	}

	@Override
	public void setup() throws SQLException {
		super.setup();
		Map<String, Column> columns = tableModel.getColumns();
		this.types = new int[columns.size()];

		StringBuilder fieldnames = new StringBuilder();
		StringBuilder fieldvalues = new StringBuilder();
		StringBuilder values = new StringBuilder();
		StringBuilder newValues = new StringBuilder();

		boolean hasCreatedClob = false;
		int i = 0;
		for (Column field : columns.values()) {
			if (field.isKey()) {
				keys.add(field);
			}
			String fieldname = field.getColumnName().toUpperCase();
			fieldnames.append(fieldname + ",");
			fieldvalues.append("?,");
			values.append(" ? as ").append(fieldname).append(",");
			newValues.append(fieldname +"=?,");
			types[i] = field.getDataTypeCode();

			if ( !hasCreatedClob && (types[i] == Types.CLOB 
					|| types[i] == TypeDescriptor.TYPECODE_CLOB)) {
				this.clob = connection.createClob();
			}
			i++;
		}

		StringBuilder conditions = new StringBuilder();
		for (int j = 0, size = keys.size(); j < size; j++) {
			conditions.append(" and ");
			conditions.append(keys.get(j).getColumnName());
			conditions.append("=?");
		}

		values.deleteCharAt(values.lastIndexOf(","));
		fieldnames.deleteCharAt(fieldnames.lastIndexOf(","));
		names = fieldnames.toString().split(",");
		length = names.length;
		fieldvalues.deleteCharAt(fieldvalues.lastIndexOf(","));
		newValues.deleteCharAt(newValues.lastIndexOf(","));

		String schema = tableModel.getLocalSchema();
		String table = tableModel.getLocalTable();

		insertStmt = connection.prepareStatement("insert into " + schema + "." + table + 
				"(" + fieldnames.toString() + ")values(" + fieldvalues.toString() + ")");

		if (conditions != null && !conditions.toString().trim().equals("")) {
			updateStmt = connection.prepareStatement("update " + schema + "." + table + 
					" set " + newValues + " where 1=1 " + conditions);
			deleteStmt = connection.prepareStatement("delete " + schema + "." + table  + 
					" where 1=1 " + conditions);
		}

	}

}
