package com.geovis.duplex.task;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractTask implements Task {
	protected final AtomicBoolean running = new AtomicBoolean(true);
	protected final Map<String, Task> tasks = new HashMap<>();
	
	@Override
	public void stop() {
		synchronized (running) {
			running.compareAndSet(true, false);
		}
	}
	
	@Override
	public void restart() {
		synchronized (running) {
			running.compareAndSet(false, true);
		}
	}
	
	@Override
	public boolean isRunning() {
		return running.get();
	}

	public AtomicBoolean getRunning() {
		return running;
	}

	public Map<String, Task> getTasks() {
		return tasks;
	}

}
