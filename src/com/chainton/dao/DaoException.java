package com.chainton.dao;

/**
 * 数据异常
 * 
 * @author fuqiang 2015-3-30
 */
public class DaoException extends Exception {

	public int errorCode;
	public Object data;

	public static final int DATAEXCEPTION = 110001; // 数据异常

	private static final long serialVersionUID = -6118241179421708236L;

	public DaoException(int type, String message) {
		super(message);
		errorCode = type;
	}

	public DaoException(int type, String message, Object object) {
		super(message);
		errorCode = type;
		data = object;
	}
}
