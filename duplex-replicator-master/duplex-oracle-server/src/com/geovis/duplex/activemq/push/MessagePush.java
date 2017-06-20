package com.geovis.duplex.activemq.push;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * 推送数据至消息中间件
 * @author Jangzo
 *
 */
public interface MessagePush {
	/**
	 * 推送单条消息至中间件
	 * @param object 消息对象
	 * @param clazz 消息类型，用于强转
	 */
	public void push(Serializable object);
	/**
	 * 关闭连接，释放资源
	 */
	public void close();
	
	/**
	 * 设定clientid和 topic name
	 * @param clientid 
	 * @param topicName
	 */
	public void setTopic(String clientid, String topicName);
	
	/**
	 * 提交数据
	 * @throws JMSException 
	 */
	public void commit() throws JMSException;
	
	/**
	 * 回滚数据
	 * @throws JMSException 
	 */
	public void rollback() throws JMSException;
	
	/**
	 * 由session 创建一个Message实体并返回
	 * @return 一个新的Message实体
	 * @throws JMSException 
	 */
	public Message createMessage() throws JMSException;
	
}
