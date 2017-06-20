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

public class TestVMRecieve implements Runnable {
	@Override
	public void run() {
		try {
			ConnectionFactory connectionFactory = 
					new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER,
					ActiveMQConnection.DEFAULT_PASSWORD, 
					"failover:(tcp://localhost:61626?wireFormat.maxInactivityDuration=10000)&amp;maxReconnectDelay=10000");
			Connection connection = connectionFactory.createConnection();
			connection.setClientID("test");
			connection.start();
			final Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
			Topic topic = session.createTopic("test");
			TopicSubscriber subscriber = session.createDurableSubscriber(topic, "test");
			subscriber.setMessageListener(new MessageListener() {
				@Override
				public void onMessage(Message message) {
					try {
						String text = message.getStringProperty("test");
						System.out.println(text);
						int i = Integer.parseInt(text);
						if (i % 50 == 0) {
							session.commit();
						}
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}
			});
		} catch (JMSException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
//		new Thread(new TestVMProduce()).start();
		(new TestVMRecieve()).run();
	}

}
