package com.geovis.duplex.activemq.broker;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.log4j.Logger;

public class EmbededBroker implements Runnable {
	private final static Logger logger = Logger.getLogger(EmbededBroker.class);
	
	private BrokerService broker;
	protected final AtomicBoolean running = new AtomicBoolean(false);
	static {
		System.setProperty("activemq.base", System.getProperty("user.dir"));
		System.setProperty("activemq.home", System.getProperty("user.dir"));
	}

	@Override
	public void run() {
		start();
	}
	
	public void start(){
		if(!this.running.compareAndSet(false, true)) {
			return;
		}
		try {
			broker = BrokerFactory.createBroker(new URI("xbean:conf/activemq.xml"));
			broker.start();
		} catch (URISyntaxException e) {
			logger.error(this, e);
		} catch (Exception e) {
			logger.error(this, e);
		}
	}
	
	public void stop() {
		if(!this.running.compareAndSet(true, false)) {
			return;
		}
		try {
			broker.stop();
		} catch (Exception e) {
			logger.error(this, e);;
		}
	}
	
	public static void main(String[] args) throws URISyntaxException, Exception {
		EmbededBroker broker = new EmbededBroker();
		broker.start();
	} 
	
}
