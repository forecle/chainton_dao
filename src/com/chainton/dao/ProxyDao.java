package com.chainton.dao;

import java.lang.reflect.Proxy;

/**

 */
public class ProxyDao {

	@SuppressWarnings("unchecked")
	public static <T> T getInstance(T t) {

		Class<?>[] interfaces;
		interfaces = t.getClass().getInterfaces();
		if ( interfaces.length == 0 && t.getClass().getSuperclass() != null) {
			interfaces = t.getClass().getSuperclass().getInterfaces();
		}
		return (T) Proxy.newProxyInstance(t.getClass().getClassLoader(), interfaces, new InvocationHandlerDao(t));
	}
}
