package com.geovis.duplex.model;

import java.util.HashMap;
import java.util.Map;

public class SchemaModel {
	private NodeModel node;
	private String remote;
	private Map<String, TableModel> tables = new HashMap<>();
	private String url;
	private String name;
	private String password;
	
	public SchemaModel() {
	}
	public String getRemote() {
		return remote;
	}
	public void setRemote(String remote) {
		this.remote = remote;
	}
	public Map<String, TableModel> getTables() {
		return tables;
	}
	public void setTables(Map<String, TableModel> tables) {
		this.tables = tables;
	}
	public NodeModel getNode() {
		return node;
	}
	public void setNode(NodeModel node) {
		this.node = node;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
}
