package com.geovis.duplex.activemq.connection;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.geovis.duplex.activemq.broker.EmbededBroker;

public class VmAMQ {
	private static final ActiveMQConnectionFactory localFactory = 
			new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER,
					ActiveMQConnection.DEFAULT_PASSWORD, 
					"vm://Broker");
	static {
		EmbededBroker broker = new EmbededBroker();
		broker.start();
		localFactory.setUseAsyncSend(true);
		localFactory.setCopyMessageOnSend(false);
		localFactory.setAlwaysSessionAsync(false);
	}
	public static Connection getConnection() throws JMSException{
		return localFactory.createConnection();
	}
}
