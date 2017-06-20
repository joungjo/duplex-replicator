package com.geovis.duplex.extract;

import com.geovis.duplex.model.Carrier;

public interface FKDataExtract extends DataExtract {
	/**
	 * 根据源表的主键查询记录
	 * @param id 主键
	 */
	public void executeQuery(String id, Carrier carrier);
}
