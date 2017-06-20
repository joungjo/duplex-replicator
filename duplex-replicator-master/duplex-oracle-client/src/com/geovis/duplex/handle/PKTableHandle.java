package com.geovis.duplex.handle;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import org.apache.log4j.Logger;

import com.geovis.duplex.model.Carrier;
import com.geovis.duplex.model.Column;
import com.geovis.duplex.model.TableModel;
import com.geovis.duplex.utils.CDCUtil;

import oracle.sql.TypeDescriptor;
/**
 * 操作主表数据，重写insert、update、delete方法
 * @since 2016-06-22
 * @author Jangzo
 * 
 */
public class PKTableHandle extends AbstractDataHandle {
	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(PKTableHandle.class);

	@SuppressWarnings("unused")
	private String pkcolumn_name; 
	private Column column;
	public PKTableHandle() { }

	public PKTableHandle(Connection connection, Column column) {
		this.connection = connection;
		this.column = column;
	}

	@Override
	protected void update(Carrier carrier) {
	}
	@Override
	protected void delete(Carrier carrier) {
	}
	
	public void setup() throws SQLException {
		String schema = column.getPkTableSchema();
		String tableName = column.getPkTableName();
		Map<String, Column> columns = CDCUtil.getTableColumns(connection, 
				schema.toUpperCase(), tableName.toUpperCase());
		pkcolumn_name = column.getPkColumnName();
		
		this.tableModel = new TableModel();
		tableModel.setLocalTable(tableName);
		tableModel.setLocalSchema(schema);
		tableModel.setColumns(columns);
		
		super.setup();
		
		StringBuilder fieldvalues = new StringBuilder();
		StringBuilder fields = new StringBuilder();

		types = new int[columns.size()];
		names = new String[columns.size()];
		
		int i = 0;
		boolean hasCreatedClob = false;
		for (String key : columns.keySet()) {
			types[i] = columns.get(key).getDataTypeCode();
			names[i] = key;
			if (!hasCreatedClob && (types[i] == Types.CLOB 
					|| types[i] == TypeDescriptor.TYPECODE_CLOB)) {
				this.clob = connection.createClob();
				hasCreatedClob = true;
			}
			i++;
			fields.append(key).append(",");
			fieldvalues.append("? ").append(",");
		}

		fieldvalues.deleteCharAt(fieldvalues.lastIndexOf(","));
		fields.deleteCharAt(fields.lastIndexOf(","));

		String sql = "insert into " + schema + "." + tableName + 
				"(" + fields + ")values(" + fieldvalues + ")";
		
		insertStmt = connection.prepareStatement(sql);
	}

}
