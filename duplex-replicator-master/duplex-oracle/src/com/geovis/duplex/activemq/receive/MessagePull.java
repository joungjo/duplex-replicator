package com.geovis.duplex.activemq.receive;

/**
 * 
 * @author Jangzo
 *
 */
public interface MessagePull {
	
	void startup();
	
	/**
	 * 关闭释放资源
	 */
	public void stop();
	/**
	 * 提交session
	 */
	public void commit();
	/**
	 * 回滚
	 */
	public void rollback();
	
}
