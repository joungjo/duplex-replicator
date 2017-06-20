package com.geovis.duplex.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Carrier implements Serializable {
	private static final long serialVersionUID = 1L;
	/*
	 * head 
	 */
	private String tablename = "";
	private String opttype = "";
	private HashMap<String, String> keys = new HashMap<String, String>();
	/*
	 * body
	 */
	private HashMap<String, StringBuffer> body = new HashMap<String, StringBuffer>();
	/*
	 * dependencies
	 */
	private Map<String, Carrier> dependencies;

	public Carrier() {
	}
	
	public Map<String, Carrier> getDependencies() {
		return dependencies;
	}

	public void setDependencies(Map<String, Carrier> dependencies) {
		this.dependencies = dependencies;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public String getOpttype() {
		return opttype;
	}

	public void setOpttype(String opttype) {
		this.opttype = opttype;
	}

	public HashMap<String, String> getKeys() {
		return keys;
	}

	public void setKeys(HashMap<String, String> keys) {
		this.keys = keys;
	}

	public HashMap<String, StringBuffer> getBody() {
		return body;
	}

	public void setBody(HashMap<String, StringBuffer> body) {
		this.body = body;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("table name:").append(tablename)
		.append(",body:").append(body)
		.append(",dependencies:").append(dependencies);
		return sb.toString();
	}

}
