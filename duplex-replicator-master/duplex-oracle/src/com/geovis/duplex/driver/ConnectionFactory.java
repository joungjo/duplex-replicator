package com.geovis.duplex.driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionFactory {
	private static String url;
	private static String user;
	private static String dba;
	private static String password;
	private static String dbaPassword;

	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url, user, password);
	}
	
	public static Connection getDbaConnection() throws SQLException {
		Properties info = new Properties();
		info.put("user",dba);
		info.put("password", dbaPassword);
		info.put("defaultRowPrefetch","15");
		info.put("internal_logon","sysdba");
		return DriverManager.getConnection(url, info);
	}

	public static String getUrl() {
		return url;
	}

	public static void setUrl(String url) {
		ConnectionFactory.url = url;
	}

	public static String getUser() {
		return user;
	}

	public static void setUser(String user) {
		ConnectionFactory.user = user;
	}

	public static String getPassword() {
		return password;
	}

	public static void setPassword(String password) {
		ConnectionFactory.password = password;
	}

	public static String getDba() {
		return dba;
	}

	public static void setDba(String dba) {
		ConnectionFactory.dba = dba;
	}

	public static String getDbaPassword() {
		return dbaPassword;
	}

	public static void setDbaPassword(String dbaPassword) {
		ConnectionFactory.dbaPassword = dbaPassword;
	}
	
}
