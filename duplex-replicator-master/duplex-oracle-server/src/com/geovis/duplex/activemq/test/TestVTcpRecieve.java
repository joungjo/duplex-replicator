package com.geovis.duplex.activemq.test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.geovis.duplex.model.Carrier;

public class TestVTcpRecieve implements Runnable {
	@Override
	public void run() {
		try {
			
			ConnectionFactory connectionFactory = 
					new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER,
					ActiveMQConnection.DEFAULT_PASSWORD, 
					"failover:(tcp://localhost:61616?wireFormat.maxInactivityDuration=10000)&amp;maxReconnectDelay=10000");
			Connection connection = connectionFactory.createConnection();
			connection.setClientID("001");
			connection.start();
			final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
			Topic topic = session.createTopic("GFGXDB_WM_1.GFGX_R_SJCP_CPYXX_CPFL");
			TopicSubscriber subscriber = session.createDurableSubscriber(topic, "GFGXDB_WM_1.GFGX_R_SJCP_CPYXX_CPFL");
			while(true) {
				 try {
						Carrier object = (Carrier)((ObjectMessage) subscriber.receive()).getObject();
						System.out.println(object);
						session.commit();
					} catch (JMSException e) {
						e.printStackTrace();
					}
			}
			/*subscriber.setMessageListener(new MessageListener() {
				@Override
				public void onMessage(Message message) {
					 try {
						Carrier object = (Carrier)((ObjectMessage) message).getObject();
						System.out.println(object);
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}
			});*/
		} catch (JMSException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
//		new Thread(new TestVMProduce()).start();
		(new TestVTcpRecieve()).run();
	}

}
