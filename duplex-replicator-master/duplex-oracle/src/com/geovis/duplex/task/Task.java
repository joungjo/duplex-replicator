package com.geovis.duplex.task;

public interface Task extends Runnable {
	
	public void stop();
	
	public void restart();
	
	public boolean isRunning();
	
}
