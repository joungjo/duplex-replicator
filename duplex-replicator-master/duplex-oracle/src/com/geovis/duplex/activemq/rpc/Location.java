package com.geovis.duplex.activemq.rpc;

import javax.jms.JMSException;

public interface Location {
	
	public void start();
	
	public void close() throws JMSException;
}
