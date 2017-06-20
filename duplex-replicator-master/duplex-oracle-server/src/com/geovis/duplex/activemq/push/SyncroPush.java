package com.geovis.duplex.activemq.push;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.log4j.Logger;

import com.geovis.duplex.activemq.connection.VmAMQ;

public class SyncroPush implements MessagePush {
	private final static Logger logger = Logger.getLogger(SyncroPush.class);
	// Session： 一个发送或接收消息的线程
	private Connection connection; 
	private Session session;
	private Topic topic;
	private MessageProducer producer; 

	public SyncroPush() {
		try {
			connection = VmAMQ.getConnection();
		} catch (JMSException e) {
			logger.error(this, e);
		}
	}

	@Override
	public void push(Serializable object) {
		try {
			ObjectMessage message = session.createObjectMessage();
			message.setObject(object);
			producer.send(message);
		} catch (JMSException e) {
			logger.error(this, e);
		}
	}

	@Override
	public void close() {
		try {
			session.close();
		} catch (JMSException e) {
			logger.error(this, e);
		} finally {
			try {
				connection.close();
			} catch (JMSException e) {
				logger.error(this, e);
			}
		}
	}

	@Override
	public void setTopic(String clientid, String topicName) {
		try {
			if (clientid != null && !"".equals(clientid.trim())) {
				connection.setClientID(clientid);
			}
			connection.start();
			
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			topic = session.createTopic(topicName + "?consumer.retroactive=true");
			
			producer = session.createProducer(topic);
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);
			producer.setTimeToLive(0);
			
		} catch (JMSException e) {
			logger.error(this, e);
		}
	}

	@Override
	public void commit() throws JMSException {
		session.commit();
	}

	@Override
	public void rollback() throws JMSException {
		session.rollback();
	}

	@Override
	public Message createMessage() throws JMSException {
		return session.createMessage();
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

}
