package com.chainton.dao;

import java.util.HashMap;
import java.util.Map;

public class DaoManager {

	private static final Map<String, BaseDao> baseDao = new HashMap<String, BaseDao>();

	// 初使化理
	static {
	}

	public static boolean containsKey(String key) {
		return baseDao.containsKey(key);
	}

	public static void put(String key, Object value) {
		baseDao.put(key, (BaseDao) value);
	}

	public static BaseDao getDao(String dao) {
		return baseDao.get(dao);
	}

}
