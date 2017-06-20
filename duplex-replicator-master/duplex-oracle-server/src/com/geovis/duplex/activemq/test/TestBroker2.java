package com.geovis.duplex.activemq.test;

import javax.jms.JMSException;

import com.geovis.duplex.activemq.broker.EmbededBroker;

public class TestBroker2 {

	public static void main(String[] args) throws JMSException, InterruptedException {
		(new EmbededBroker()).run();
		(new TestVMRecieve()).run();
		(new TestVMProduce()).run();
	}

}
