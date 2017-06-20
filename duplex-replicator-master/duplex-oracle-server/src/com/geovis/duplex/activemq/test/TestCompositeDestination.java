package com.geovis.duplex.activemq.test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class TestCompositeDestination {

	public static void main(String[] args) throws JMSException {
		ConnectionFactory connectionFactory = 
				new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER,
				ActiveMQConnection.DEFAULT_PASSWORD, 
				"failover:(tcp://localhost:61616?wireFormat.maxInactivityDuration=10000)&amp;maxReconnectDelay=10000");
		Connection connection = connectionFactory.createConnection();
		connection.setClientID("001");
		connection.start();
		final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Topic topic = session.createTopic("test,test1");
		TopicSubscriber subscriber = session.createDurableSubscriber(topic, "test");
		subscriber.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message message) {
				try {
					int text = message.getIntProperty("test");
					System.out.println(text);
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		});
	}

}
