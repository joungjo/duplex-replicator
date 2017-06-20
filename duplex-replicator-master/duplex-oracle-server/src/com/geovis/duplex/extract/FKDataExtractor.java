package com.geovis.duplex.extract;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.geovis.duplex.model.Carrier;
import com.geovis.duplex.model.Column;
import com.geovis.duplex.utils.CDCUtil;

public class FKDataExtractor extends Extractor implements FKDataExtract {
	private final static Logger logger = Logger.getLogger(FKDataExtractor.class);

	private Column column;

	public FKDataExtractor(Connection connection, Column column) {
		this.connection = connection;
		this.column = column;
		this.schema = column.getPkTableSchema();
		this.tableName = column.getPkTableName();
	}

	@Override
	public void startup() {
		if(!this.running.compareAndSet(false, true)) {
			return;
		}
		
		StringBuilder sql = new StringBuilder();
		sql.append( "select * from ")
		.append(schema).append(".")
		.append(tableName).append(" where ")
		.append(column.getPkColumnName()).append("=?");
		try {
			this.prepare(sql.toString());
		} catch (SQLException e) {
			logger.error(this, e);
		}
		
		this.columns = CDCUtil.getTableColumns(connection, schema, tableName);
		initUDTs(columns);
		for (Column column : columns.values()) {
			String fkColumnName = column.getFkColumnName();
			if (fkColumnName != null && !fkColumnName.trim().equals("")) {
				FKDataExtractor sourceExtractor = 
						new FKDataExtractor(connection, column);
				sourceExtractor.startup();
				sourceExtractors.put(fkColumnName, sourceExtractor);
			}
		}
	}

	@Override
	public void executeQuery(String id, Carrier carrier) {
		try {
			this.stmt.setString(1, id);

			time = System.currentTimeMillis();
			increase = 0;
			rs = stmt.executeQuery();
			ResultSetMetaData resultMetaData = rs.getMetaData();
			int fieldCount = resultMetaData.getColumnCount();
			if (rs.next()) {
				Carrier carrier2 = fetchVaues(resultMetaData, fieldCount);
				if (carrier.getDependencies() == null) {
					carrier.setDependencies(new HashMap<String, Carrier>());
				}
				carrier.getDependencies().put(column.getColumnName(), carrier2);
				increase++;
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error(this, e);
				}
			}
		} catch (SQLException e) {
			logger.error(this, e);
		}

	}
	
	@Override
	protected void fKeysAndUdts(Carrier carrier) throws SQLException {
		for (String fkey : sourceExtractors.keySet()) {
			sourceExtractors.get(fkey).executeQuery(rs.getString(fkey), carrier);
		}
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

	@Override
	protected void clearSub() throws SQLException {	}

	@Override
	protected void extendSub() throws SQLException { }

	@Override
	protected Carrier cover(HashMap<String, StringBuffer> hm) {
		Carrier carrier = new Carrier();
		carrier.setOpttype("I");
		carrier.setTablename(tableName);
		carrier.setBody(hm);
		return carrier;
	}

	@Override
	public void stop() {
		try {
			if (rs != null) 
				this.rs.close();
			if (stmt != null) 
				this.stmt.close();
		} catch (SQLException e) {
			logger.error(this, e);;
		}
	}

}
