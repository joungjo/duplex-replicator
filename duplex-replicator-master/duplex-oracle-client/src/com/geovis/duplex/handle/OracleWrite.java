package com.geovis.duplex.handle;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;

import org.apache.log4j.Logger;

import com.geovis.duplex.activemq.receive.SynchronousReceive;
import com.geovis.duplex.model.Carrier;
import com.geovis.duplex.model.Column;
import com.geovis.duplex.model.TableModel;
/**
 * 
 * @author Jangzo
 *
 */
public class OracleWrite extends AbstractDataHandle {
	private final static Logger logger = Logger.getLogger(OracleWrite.class);

	public OracleWrite() {}

	public OracleWrite(TableModel tableModel) 
			throws SQLException {
		this.tableModel = tableModel;
	}

	public OracleWrite(TableModel tableModel, SynchronousReceive pull) 
			throws SQLException {
		this.tableModel = tableModel;
		this.puller = pull;
	}

	protected void update(Carrier carrier) {
		Map<String, StringBuffer> fields = carrier.getBody();
		try {
			int columnAmount = names.length;
			a : for(int i = 0; i < columnAmount; i++){
				String name = names[i];
				String oldName = "UO_" + names[i];
				StringBuffer fieldvalue = fields.get(name);
				StringBuffer oldValue;

				boolean isNull = fieldvalue.toString().trim().equals("");

				if (types[i] == Types.BLOB || types[i] == Types.CLOB
						|| types[i] == Types.LONGVARBINARY) {
					columnAmount--; 
					if (!isNull) {
						byte[] content = parseLob(fieldvalue);
						updateStmt.setBytes(i + 1, content);
					} else {
						updateStmt.setNull(i +1, types[i]);
					}
					continue a;
				}

				if (!isNull) {
					if  (types[i] == Types.TIME || types[i] == Types.TIMESTAMP
							|| types[i] == Types.TIMESTAMP) {
						updateStmt.setTimestamp(i + 1, 
								new Timestamp(Long.parseLong(fieldvalue.toString())));
					} else {
						updateStmt.setObject(i + 1, fieldvalue.toString());
					}
				} else {
					updateStmt.setNull(i +1, types[i]);
				}

				if (!(oldValue = fields.get(oldName)).toString().trim().equals("")) {
					if  (types[i] == Types.TIME || types[i] == Types.TIMESTAMP
							|| types[i] == Types.TIMESTAMP) {
						updateStmt.setTimestamp(i + 1 + columnAmount, 
								new Timestamp(Long.parseLong(oldValue.toString())));
					} else {
						updateStmt.setObject(i + 1 + columnAmount, oldValue.toString());
					}
				} else {
					updateStmt.setNull(i + 1 + columnAmount, types[i]);
				}
			}
			updateStmt.addBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(tableModel, e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(tableModel, e);
		}

	}

	protected void delete(Carrier carrier) {
		Map<String, StringBuffer> fields = carrier.getBody();
		try {
			a : for(int i = 0, j = 1; i < names.length; i++, j++){
				String name = names[i];
				StringBuffer fieldvalue;

				if (types[i] == Types.BLOB || types[i] == Types.CLOB
						|| types[i] == Types.LONGVARBINARY) {
					j--;
					continue a;
				} 
				if (fields.containsKey(name) && (fieldvalue = fields.get(name)) != null) {
					deleteStmt.setObject(j, fieldvalue.toString());
				} else {
					deleteStmt.setNull(j, types[i]);
				}
			}
		deleteStmt.addBatch();
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
		StringBuilder newValues = new StringBuilder();
		StringBuilder conditions = new StringBuilder();

		int i = 0;
		for (Column field : columns.values()) {
			String fieldname = field.getColumnName().toUpperCase();
			fieldnames.append(fieldname + ",");
			fieldvalues.append("?,");
			newValues.append(fieldname +"=?,");
			types[i] = field.getDataTypeCode();
			if (!(types[i] == Types.BLOB || types[i] == Types.CLOB
					|| types[i] == Types.LONGVARBINARY)) {
				conditions.append(" and " + fieldname + "=?");
			}
			i++;
		}

		fieldnames.deleteCharAt(fieldnames.lastIndexOf(","));
		names = fieldnames.toString().split(",");
		fieldvalues.deleteCharAt(fieldvalues.lastIndexOf(","));
		newValues.deleteCharAt(newValues.lastIndexOf(","));
		String schema = tableModel.getLocalSchema();
		String tablename = tableModel.getLocalTable();
		insertStmt = connection.prepareStatement("insert into " + schema + "." + tablename + 
				"(" + fieldnames.toString() + ")values(" + fieldvalues.toString() + ")");
		updateStmt = connection.prepareStatement("update " + schema + "." + tablename + 
				" set " + newValues + " where 1=1 " + conditions);
		if (conditions != null && !conditions.toString().trim().equals("")) {
			deleteStmt = connection.prepareStatement("delete " + schema + "." + tablename  + 
					" where 1=1 " + conditions);
		}
	}

}
