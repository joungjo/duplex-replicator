package com.geovis.duplex.model;

import java.util.List;

public class NodeModel {
	private String address;
	private int port;
	private List<SchemaModel> schemaModels;

	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public List<SchemaModel> getSchemaModels() {
		return schemaModels;
	}
	public void setSchemaModels(List<SchemaModel> schemaModels) {
		this.schemaModels = schemaModels;
	}
}
