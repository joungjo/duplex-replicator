package com.geovis.duplex.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.struts2.ServletActionContext;

import com.geovis.duplex.OracleServer;
import com.geovis.duplex.OracleServer.SchemaTask;
import com.geovis.duplex.OracleServer.TableTask;
import com.geovis.duplex.model.SchemaModel;
import com.geovis.duplex.model.TableModel;
import com.geovis.duplex.task.Task;

import net.sf.json.JSONArray;

public class ConsoleAction {
	private String table;
	private String schema;
	private OracleServer server = OracleServer.newInstance();
	
	public void getSchemas() throws IOException{
		Map<String, SchemaTask> schemaTasks = server.getSchemaTasks();
		List<Schema> schemas = new ArrayList<>();
		for(String key : schemaTasks.keySet()) {
			SchemaTask schemaTask = schemaTasks.get(key);
			SchemaModel schemaModel = schemaTask.getSchemaModel();
			Schema schema = new Schema();
			schema.setName(schemaModel.getName());
			Map<String, Task> tasks = schemaTask.getTasks();
			List<Table> tables = new ArrayList<>();
			for (String k : tasks.keySet()) {
				Task task = tasks.get(k);
				if (task instanceof TableTask) {
					Table table = new Table();
					TableTask tableTask = (TableTask)task;
					TableModel tableModel = tableTask.getTableModel();
					table.setName(tableModel.getLocalTable());
					table.setStatus(task.isRunning());
					tables.add(table);
				}
			}
			schema.setTables(tables);
			schemas.add(schema);
		}
		HttpServletResponse response = ServletActionContext.getResponse();
		PrintWriter writer = response.getWriter();
		writer.write(JSONArray.fromObject(schemas).toString());
	}
	
	public void addTable() throws SQLException{
		Map<String, SchemaTask> schemaTasks = server.getSchemaTasks();
		SchemaTask schemaTask = schemaTasks.get(schema);
		SchemaModel schemaModel = schemaTask.getSchemaModel();
		schemaModel.getTables().put(table, server.civilizeTable(schemaModel, table));
		schemaTask.refresh();
	}
	
	public void stopThread(){
		Map<String, SchemaTask> schemaTasks = server.getSchemaTasks();
		SchemaTask schemaTask = schemaTasks.get(schema);
		Map<String, Task> tasks = schemaTask.getTasks();
		String key = schema + "." + table;
		Task task = tasks.get(key);
		if (task != null) {
			task.stop();
			Map<String, Thread> threads = server.getThreads();
			if (threads.containsKey(key)) {
				while(threads.get(key).isAlive())
					task.stop();
				threads.remove(key);
			}
		}
	}
	
	public void startExtractor(){
		Map<String, SchemaTask> schemaTasks = server.getSchemaTasks();
		SchemaTask schemaTask = schemaTasks.get(schema);
		Map<String, Task> tasks = schemaTask.getTasks();
		String key = schema + "." + table;
		tasks.get(key).restart();
		schemaTask.refresh();
	}
	
	public void rebuildExtractor() throws SQLException{
		Map<String, SchemaTask> schemaTasks = server.getSchemaTasks();
		SchemaTask schemaTask = schemaTasks.get(schema);
		SchemaModel schemaModel = schemaTask.getSchemaModel();
		TableModel tableModel = schemaModel.getTables().get(table);
		schemaModel.getTables().remove(table);
		Map<String, Task> tasks = schemaTask.getTasks();
		String key = schema + "." + table;
		Task task = tasks.get(key);
		if (task != null) {
			task.stop();
			Map<String, Thread> threads = server.getThreads();
			if (threads.containsKey(key)) {
				while(threads.get(key).isAlive())
					task.stop();
				threads.remove(key);
			}
			tasks.remove(key);
		}
		server.dropCDC(tableModel);
		server.createCDC(tableModel);
		schemaModel.getTables().put(table, tableModel);
		schemaTask.refresh();
	}
	
	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public class Schema {
		private String name;
		private List<Table> tables;
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public List<Table> getTables() {
			return tables;
		}
		public void setTables(List<Table> tables) {
			this.tables = tables;
		}
	}
	
	public class Table {
		private String name;
		private boolean status;
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public boolean isStatus() {
			return status;
		}
		public void setStatus(boolean status) {
			this.status = status;
		}
	}

}



