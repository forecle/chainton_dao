package com.chainton.dao;

import com.chainton.dao.BaseDao;

public interface ITeaminOauth extends BaseDao {

	/**
	 * 查看指定应用的名称是否存在
	 * 
	 * @return
	 */
	public boolean existAppleName(String appleName);

}
