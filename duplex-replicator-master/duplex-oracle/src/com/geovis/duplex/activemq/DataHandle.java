package com.geovis.duplex.activemq;

import java.sql.SQLException;

import javax.jms.JMSException;
import javax.jms.Message;

public interface DataHandle {
	/**
	 * 处理数据
	 * @throws JMSException 
	 * @throws SQLException 
	 */
	public void handle(Message message) throws JMSException, SQLException;
	
	public void setup() throws SQLException;
}
