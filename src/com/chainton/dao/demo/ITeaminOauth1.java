package com.chainton.dao.demo;

import com.chainton.dao.BaseDao;

public interface ITeaminOauth1 extends BaseDao {

	/**
	 * 查看指定应用的名称是否存在
	 * 
	 * @return
	 */
	public boolean existAppleName(String appleName);

}
