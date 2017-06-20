package com.geovis.duplex.activemq.receive;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

import com.geovis.duplex.activemq.DataHandle;
import com.geovis.duplex.handle.HandleByKey;
import com.geovis.duplex.handle.OracleWrite;
import com.geovis.duplex.model.Column;
import com.geovis.duplex.model.TableModel;

public class SynchronousReceive implements MessagePull {
	private final static Logger logger = Logger.getLogger(SynchronousReceive.class);
	
	protected Connection connection; 
	protected Session session;
	protected TopicSubscriber consumer;
	protected DataHandle handle;
	protected TableModel tableModel;
	
	protected final AtomicBoolean running = new AtomicBoolean(false);
	
	public SynchronousReceive(TableModel tableModel) {
		this.tableModel = tableModel;
	}
	
	@Override
	public void startup() {
		if(!this.running.compareAndSet(false, true)) {
			return;
		}
		String remoteSchema = tableModel.getSchema().getRemote();
		String remoteTable = tableModel.getRemoteTable();
		String url = tableModel.getUrl();
		String ip = tableModel.getSchema().getNode().getAddress();
		int port = tableModel.getSchema().getNode().getPort();
		StringBuilder sb = new StringBuilder("failover:(tcp://");
		sb.append(ip).append(":").append(port)
		.append("?wireFormat.maxInactivityDuration=10000)&amp;maxReconnectDelay=10000");
		ConnectionFactory factory = 
				new ActiveMQConnectionFactory(
						ActiveMQConnection.DEFAULT_USER,
						ActiveMQConnection.DEFAULT_PASSWORD, 
						sb.toString());
		try {
			connection = factory.createConnection();
			connection.setClientID(url + "_" + tableModel.getLocalSchema());
			connection.start();
			handle = getDataHandle();
			handle.setup();
			session = connection.createSession(true, Session.SESSION_TRANSACTED);
			Topic topic = session.createTopic(remoteSchema + "." + remoteTable);
			consumer = session.createDurableSubscriber(topic, remoteSchema + "." + remoteTable);
			while(running.get()){
				handle.handle(consumer.receive());
			}
			try {
				consumer.close();
				session.close();
			} catch (JMSException e) {
				logger.error(e, e.getCause());
			} finally {
				try {
					connection.close();
				} catch (JMSException e) {
					logger.error(e, e.getCause());
				}
			}
		} catch (JMSException e) {
			logger.error(e, e.getCause());
		} catch (SQLException e) {
			logger.error(e, e.getCause());
		}
	}

	private DataHandle getDataHandle() throws SQLException {
		for (Column column : tableModel.getColumns().values()) {
			if (column.isKey()) {
				return new HandleByKey(tableModel, this);
			}
		}
		return new OracleWrite(tableModel, this);
	}

	public void setHandle(DataHandle handle) {
		this.handle = handle;
	}

	@Override
	public void stop() {
		running.compareAndSet(true, false);
	}

	@Override
	public void commit() {
		try {
			session.commit();
		} catch (JMSException e) {
			logger.error(e, e.getCause());
		}
	}

	@Override
	public void rollback() {
		try {
			session.rollback();
		} catch (JMSException e) {
			logger.error(e, e.getCause());
		}
	}

}
