package com.chainton.dao;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.sql.Connection;

/**
 * @author qfu 2013-9-23
 */
public class InvocationHandlerDao implements InvocationHandler {

	private Object objDao;

	public <T> InvocationHandlerDao(T t) {
		this.objDao = t;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

		boolean isRecover = false;
		Connection connection = null;
		Parameter[] paras = method.getParameters();

		for (int i = 0; i < paras.length; i++) {
			if (args[i] == null && paras[i].getType().isAssignableFrom(Connection.class)) {
				connection = DatabaseConnection.getConnection();
				args[i] = connection;
				isRecover = true;
			}
		}

		Object obj = null;
		try {
			obj = method.invoke(objDao, args);
		} catch (InvocationTargetException invocationExc) {
			if (connection != null)
				connection.rollback();
			throw invocationExc.getTargetException();
		} catch (Exception e) {
			if (connection != null)
				connection.rollback();
			throw e;
		} finally {
			if (isRecover && connection != null) {
				try {
					DatabaseConnection.close(connection);
				} catch (Exception e) {
				}
			}
		}

		return obj;
	}
}
