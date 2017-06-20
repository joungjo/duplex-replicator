package com.geovis.duplex.handle;

import com.geovis.duplex.activemq.DataHandle;

public interface DatabaseHandle extends DataHandle {
	/**
	 * 执行清理任务
	 */
	public void clear();
	
	/**
	 * 关闭释放资源
	 */
	public void close();
	
	/**
	 * 执行操作
	 */
	public void execute();
	
}
