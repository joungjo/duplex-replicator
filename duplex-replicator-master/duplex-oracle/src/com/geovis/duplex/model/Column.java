package com.geovis.duplex.model;

import java.io.Serializable;

public class Column implements Serializable {
	private static final long serialVersionUID = -7623951129436744440L;

	private String columnName;

	private boolean isKey;

	private String dataType;

	private int dataTypeCode;

	private boolean nullable;

	private int length;

	private String pkTableSchema;

	private String pkTableName;

	private String pkColumnName;

	private String fkColumnName;

	private String fkTableName;

	private String fkTableSchem;

	public Column() {}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public boolean isKey() {
		return isKey;
	}

	public void setKey(boolean isKey) {
		this.isKey = isKey;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public int getDataTypeCode() {
		return dataTypeCode;
	}

	public void setDataTypeCode(int dataTypeCode) {
		this.dataTypeCode = dataTypeCode;
	}

	public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public String getPkTableSchema() {
		return pkTableSchema;
	}

	public void setPkTableSchema(String pkTableSchema) {
		this.pkTableSchema = pkTableSchema;
	}

	public String getPkTableName() {
		return pkTableName;
	}

	public void setPkTableName(String pkTableName) {
		this.pkTableName = pkTableName;
	}

	public String getPkColumnName() {
		return pkColumnName;
	}

	public void setPkColumnName(String pkColumnName) {
		this.pkColumnName = pkColumnName;
	}

	public String getFkColumnName() {
		return fkColumnName;
	}

	public void setFkColumnName(String fkColumnName) {
		this.fkColumnName = fkColumnName;
	}

	public String getFkTableName() {
		return fkTableName;
	}

	public void setFkTableName(String fkTableName) {
		this.fkTableName = fkTableName;
	}

	public String getFkTableSchem() {
		return fkTableSchem;
	}

	public void setFkTableSchem(String fkTableSchem) {
		this.fkTableSchem = fkTableSchem;
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{columnName:").append(columnName)
		.append(",nullable:").append(nullable)
		.append(",dataType:").append(dataType)
		.append(",fkColumnName:").append(fkColumnName)
		.append(",fkTableName:").append(fkTableName)
		.append(",fkTableSchem:").append(fkTableSchem)
		.append(",pkColumnName:").append(pkColumnName)
		.append(",pkTableName:").append(pkTableName)
		.append(",pkTableSchema:").append(pkTableSchema)
		.append(",length:").append(length)
		.append(",dataTypeCode:").append(dataTypeCode)
		.append(",isKey:").append(isKey).append("}");
		return sb.toString();
	}
}