package com.geovis.duplex.extract;

import java.sql.SQLException;

/**
 * 从数据库读取需要发送的数据
 * @author Jangzo
 *
 */
public interface DataExtract {
	/**
	 * 执行只读查询
	 * @param sql 查询sql语句
	 * @throws SQLException 
	 */
	public void executeQuery() throws SQLException;
	/**
	 * 关闭释放数据库资源
	 */
	public void stop();
	/**
	 * 清除对象属性缓存
	 */
	public void clear();
	/**
	 * 准备查询
	 * @throws SQLException 
	 */
	void prepare(String sql) throws SQLException;
}
