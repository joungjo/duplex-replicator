package com.geovis.duplex.activemq.rpc;

import javax.jms.JMSException;
import javax.jms.Message;

public interface DemandHandle {
	
	public String handle(Message message) throws JMSException;
}
