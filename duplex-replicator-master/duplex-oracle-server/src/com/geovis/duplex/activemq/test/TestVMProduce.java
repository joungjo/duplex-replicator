package com.geovis.duplex.activemq.test;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import com.geovis.duplex.activemq.broker.EmbededBroker;
import com.geovis.duplex.activemq.connection.VmAMQ;

public class TestVMProduce implements Runnable {

	@Override
	public void run() {
		try {
			Connection connection = VmAMQ.getConnection();
			Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
			Topic topic = session.createTopic("test");
			MessageProducer producer = session.createProducer(topic);

			/*ConnectionFactory connectionFactory2 = 
					new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER,
							ActiveMQConnection.DEFAULT_PASSWORD, 
							"failover:(tcp://localhost:61626?wireFormat.maxInactivityDuration=10000)&amp;maxReconnectDelay=10000");

			Connection connection2 = connectionFactory2.createConnection();
			Session session2 = connection2.createSession(true, Session.SESSION_TRANSACTED);
			Topic topic2 = session2.createTopic("test");
			MessageProducer producer2 = session2.createProducer(topic2);*/

			producer.setDeliveryMode(DeliveryMode.PERSISTENT);
			for (int i = 0; i < 10000; i++) {
				if (i % 2 == 1) {
					Message message = session.createMessage();
					message.setStringProperty("test", "" + i);
					producer.send(message);
					session.commit();
				} /*else {
					Message message2 = session2.createMessage();
					message2.setStringProperty("test", "" + i);
					producer2.send(message2);
					session2.commit();
				}*/
				Thread.sleep(600);
			}
			session.close();
//			session2.close();
		} catch (JMSException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		EmbededBroker broker = new EmbededBroker();
		broker.start();
		TestVMProduce produce = new TestVMProduce();
		produce.run();
	}

}
