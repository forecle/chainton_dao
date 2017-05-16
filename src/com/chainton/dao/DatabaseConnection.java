package com.chainton.dao;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.sql.DataSource;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mchange.v2.c3p0.DataSources;
//import org.apache.tomcat.jdbc.pool.DataSource;
//import org.apache.tomcat.jdbc.pool.PoolProperties;
import com.mysql.jdbc.StringUtils;

public class DatabaseConnection {
	public static class CallRegisterOut {
		public int type;
		public Object out;
		public String parameterName;

		public CallRegisterOut(int type) {
			this.type = type;
		}

		public CallRegisterOut(int type, String parameterName) {
			this.parameterName = parameterName;
			this.type = type;
		}
	}

	// private Context envContext = null;
	private static DataSource dataSource = null;
	private static DatabaseConnection connectDb;
	public static int startCount = 0;
	public static int stopCount = 0;

	// private boolean DEBUG = true;

	public void init(String databaseurl, String username, String passwd) {

		Properties c3propes = new Properties();
		c3propes.put("c3p0.initialPoolSize", "2");
		c3propes.put("c3p0.minPoolSize", "2");
		c3propes.put("c3p0.maxPoolSize", "5");
		c3propes.put("c3p0.acquireIncrement", "60");
		c3propes.put("c3p0.acquireRetryDelay", "1000");
		c3propes.put("c3p0.autoCommitOnClose", "false");
		c3propes.put("c3p0.checkoutTimeout", "10000");
		c3propes.put("c3p0.idleConnectionTestPeriod", "360");
		c3propes.put("c3p0.maxIdleTime", "600");
		c3propes.put("c3p0.testConnectionOnCheckin", "true");
		c3propes.put("c3p0.driverClass", "com.mysql.jdbc.Driver");

		Properties jdbcpropes = new Properties();
		jdbcpropes.put("driverClass", "com.mysql.jdbc.Driver");
		jdbcpropes.put("jdbcUrl", databaseurl);
		jdbcpropes.put("user", username);
		jdbcpropes.put("password", passwd);
		jdbcpropes.put("jdbc.automaticTestTable", "c3p0TestTable");

		DataSource unPooled = null;
		try {
			unPooled = DataSources.unpooledDataSource(databaseurl, jdbcpropes);
		} catch (SQLException e) {

			e.printStackTrace();
		}
		try {
			dataSource = DataSources.pooledDataSource(unPooled, c3propes);
		} catch (SQLException e) {

			e.printStackTrace();
		}

	}

	public static DatabaseConnection newInstance() {

		synchronized (DatabaseConnection.class) {
			if (connectDb == null) {
				connectDb = new DatabaseConnection();
			}
		}
		return connectDb;
	}

	/**
	 * 获取指数据库用户的数据库连接
	 * 
	 * @param userName
	 * @param passwd
	 * @return
	 */
	public static Connection getConnection(String userName, String passwd) {

		try {
			return dataSource.getConnection(userName, passwd);
		} catch (SQLException e) {

			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 获取数据库连接
	 * 
	 * @return
	 */
	public static Connection getConnection() {

		try {
			// long start = System.currentTimeMillis();
			Connection con = dataSource.getConnection();
			con.setAutoCommit(false);
			startCount++;
			// System.out.println("get connection Time = "
			// +(System.currentTimeMillis() - start));
			return con;
		} catch (SQLException e) {

			e.printStackTrace();

		}
		return null;

	}

	/**
	 * 插入，删除，更新数据
	 * 
	 * @param sql
	 * @param arg
	 * @return
	 * @throws DaoException
	 */
	public static int executSqlUpdate(Connection connection, boolean autocommit, String sql, Object... arg) throws DaoException {

		PreparedStatement prepare;
		int result = 0;
		try {
			connection.setAutoCommit(autocommit);
			prepare = connection.prepareStatement(sql);
			setSqlPara(prepare, arg);
			result = prepare.executeUpdate();
		} catch (SQLException e) {
			throw new DaoException(DaoException.DATAEXCEPTION, "数据异常" + e.getMessage());
		}

		return result;
	}

	/**
	 * 插入，删除，更新数据
	 * 
	 * @param sql
	 * @param arg
	 * @return
	 * @throws DaoException
	 */
	public static int executSqlUpdate(Connection connection, String sql, Object... arg) throws DaoException {

		PreparedStatement prepare;
		int result = 0;
		try {
			connection.setAutoCommit(false);
			prepare = connection.prepareStatement(sql);
			setSqlPara(prepare, arg);
			// if (DEBUG)
			// System.out.println("sql = " + sql);
			result = prepare.executeUpdate();
		} catch (SQLException e) {

			// e.printStackTrace();
			throw new DaoException(DaoException.DATAEXCEPTION, "数据异常" + e.getMessage());
		}

		return result;
	}

	/**
	 * 存储过程的调用
	 * 
	 * @param sql
	 * @param arg
	 * @return
	 * @throws DaoException
	 */
	public static ResultSet executSqlProcedure(Connection connection, boolean autocommiet, String sql, Object... arg) throws DaoException {

		CallableStatement prepare;
		ResultSet result = null;
		try {
			connection.setAutoCommit(autocommiet);
			prepare = connection.prepareCall(sql);
			setSqlPara(prepare, arg);
			// boolean bol =
			prepare.execute();
			result = prepare.getResultSet();
			// printCallableStatement(prepare, bol) ;
		} catch (SQLException e) {
			// e.printStackTrace();
			throw new DaoException(DaoException.DATAEXCEPTION, "数据异常");

		}

		return result;
	}

	public static ResultSet executeCallOut(Connection connection, String sql, Object... arg) {

		CallableStatement prepare;
		ResultSet result = null;
		try {
			prepare = connection.prepareCall(sql);

			setCallPara(prepare, arg);
			prepare.execute();
			result = prepare.getResultSet();

			for (int i = 0; i < arg.length; i++) {
				if (arg[i] instanceof CallRegisterOut) {
					CallRegisterOut call = (CallRegisterOut) arg[i];
					try {
						switch (call.type) {
						case Types.VARCHAR:
						case Types.CHAR:
							if (StringUtils.isNullOrEmpty(call.parameterName))
								call.out = prepare.getString(i + 1);
							else
								call.out = prepare.getString(call.parameterName);
							break;
						case Types.INTEGER:
						case Types.SMALLINT:
						case Types.TINYINT:
							if (StringUtils.isNullOrEmpty(call.parameterName))
								call.out = prepare.getInt(i + 1);
							else
								call.out = prepare.getString(call.parameterName);
							break;
						case Types.DECIMAL:
						case Types.DOUBLE:
							if (StringUtils.isNullOrEmpty(call.parameterName))
								call.out = prepare.getDouble(i + 1);
							else
								call.out = prepare.getDouble(call.parameterName);
							break;
						case Types.FLOAT:
							if (StringUtils.isNullOrEmpty(call.parameterName))
								call.out = prepare.getFloat(i + 1);
							else
								call.out = prepare.getFloat(call.parameterName);
							break;
						case Types.DATE:
							if (StringUtils.isNullOrEmpty(call.parameterName))
								call.out = prepare.getDate(i + 1);
							else
								call.out = prepare.getDate(call.parameterName);
							break;
						case Types.TIMESTAMP:
							if (StringUtils.isNullOrEmpty(call.parameterName))
								call.out = prepare.getTimestamp(i + 1);
							else
								call.out = prepare.getTimestamp(call.parameterName);
							break;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 存储过程的调用
	 * 
	 * @param sql
	 * @param arg
	 * @return
	 * @throws DaoException
	 */
	public static ResultSet executSqlProcedure(Connection connection, String sql, Object... arg) throws DaoException {

		CallableStatement prepare;
		ResultSet result = null;
		try {
			connection.setAutoCommit(false);
			prepare = connection.prepareCall(sql);
			setSqlPara(prepare, arg);
			// boolean bol =
			prepare.execute();

			result = prepare.getResultSet();

			// printCallableStatement(prepare, bol) ;
		} catch (SQLException e) {
			// e.printStackTrace();
			throw new DaoException(DaoException.DATAEXCEPTION, "数据异常");

		}

		return result;
	}

	/**
	 * 存储过程的调用
	 * 
	 * @param sql
	 * @param arg
	 * @return
	 * @throws DaoException
	 */
	@SuppressWarnings({ "unchecked" })
	public static <T> T executSqlProcedureByAutocommit(Connection connection, Class<T> clases, T defau, Boolean autoCommit, String sql, Object... arg) {

		Connection connection2 = connection;
		CallableStatement prepare;
		ResultSet result = null;
		try {
			if (connection == null) {
				connection2 = DatabaseConnection.getConnection();
			}
			connection2.setAutoCommit(autoCommit);
			prepare = connection2.prepareCall(sql);
			setSqlPara(prepare, arg);
			prepare.execute();
			result = prepare.getResultSet();
			try {
				if (result.next()) {
					if (String.class.isAssignableFrom(clases)) {
						return (T) result.getString(1);
					} else if (Integer.class.isAssignableFrom(clases)) {
						return (T) (Integer) result.getInt(1);
					} else if (Long.class.isAssignableFrom(clases)) {
						return (T) (Long) result.getLong(1);
					} else if (Float.class.isAssignableFrom(clases)) {
						return (T) (Float) result.getFloat(1);
					} else if (Double.class.isAssignableFrom(clases)) {
						return (T) (Double) result.getDouble(1);
					} else if (Boolean.class.isAssignableFrom(clases)) {
						return (T) (Boolean) result.getBoolean(1);
					} else if (Timestamp.class.isAssignableFrom(clases)) {
						return (T) (Timestamp) result.getTimestamp(1);
					} else if (JSONObject.class.isAssignableFrom(clases)) {
						return (T) JSONObject.parseObject(result.getString(1));
					} else if (JSONArray.class.isAssignableFrom(clases)) {
						JSONArray jr = JSONArray.parseArray(result.getString(1));
						if (jr == null) {
							return (T) new JSONArray();
						} else
							return (T) jr;
					} else if (Map.class.isAssignableFrom(clases)) {
						return (T) (Map<String, String>) ResultSetMapper.mapRersultSetToMap(result);
					} else
						return ResultSetMapper.mapRersultSetToObjectOneLine(result, clases);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (SQLException e) {
			e.printStackTrace();

		} finally {
			if (connection == null)
				close(connection2);
		}

		return (T) defau;
	}

	public static ResultSet executSqlProcedureByAutocommit(Connection connection, Boolean autoCommit, String sql, Object... arg) {

		Connection connection2 = connection;
		CallableStatement prepare;
		ResultSet result = null;
		try {
			if (connection == null) {
				connection2 = DatabaseConnection.getConnection();
			}
			connection2.setAutoCommit(autoCommit);
			prepare = connection2.prepareCall(sql);
			setSqlPara(prepare, arg);
			prepare.execute();
			result = prepare.getResultSet();

		} catch (SQLException e) {
			e.printStackTrace();

		} finally {
			if (connection == null)
				close(connection2);
		}

		return result;
	}

	/**
	 * 存储过程的调用
	 * 
	 * @param sql
	 * @param arg
	 * @return
	 * @throws DaoException
	 */
	@SuppressWarnings({ "unchecked" })
	public static <T> List<T> executSqlProcedureByAutocommitResultList(Connection connection, Class<T> clases, Boolean autoCommit, String sql, Object... arg) {

		Connection connection2 = connection;
		CallableStatement prepare;
		ResultSet result = null;
		try {
			if (connection == null) {
				connection2 = DatabaseConnection.getConnection();
			}
			connection2.setAutoCommit(autoCommit);
			prepare = connection2.prepareCall(sql);
			setSqlPara(prepare, arg);
			prepare.execute();
			result = prepare.getResultSet();

			if (Map.class.isAssignableFrom(clases)) {
				return (List<T>) ResultSetMapper.mapRersultSetToListMap(result);
			} else if (String.class.isAssignableFrom(clases) || Integer.class.isAssignableFrom(clases) || Long.class.isAssignableFrom(clases) || Float.class.isAssignableFrom(clases)
					|| Double.class.isAssignableFrom(clases) || Boolean.class.isAssignableFrom(clases)) {
				List<T> l = new ArrayList<>();
				try {
					while (result.next()) {
						T t = null;
						if (String.class.isAssignableFrom(clases)) {
							t = (T) result.getString(1);
						} else if (Integer.class.isAssignableFrom(clases)) {
							t = (T) (Integer) result.getInt(1);
						} else if (Long.class.isAssignableFrom(clases)) {
							t = (T) (Long) result.getLong(1);
						} else if (Float.class.isAssignableFrom(clases)) {
							t = (T) (Float) result.getFloat(1);
						} else if (Double.class.isAssignableFrom(clases)) {
							t = (T) (Double) result.getDouble(1);
						} else if (Boolean.class.isAssignableFrom(clases)) {
							t = (T) (Boolean) result.getBoolean(1);
						} else {
							t = (T) result.getObject(1);
						}
						l.add(t);
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				return l;
			} else {
				return ResultSetMapper.mapRersultSetToObject(result, clases);
			}

		} catch (SQLException e) {
			e.printStackTrace();

		} finally {
			if (connection == null)
				close(connection2);
		}
		return null;

	}

	public static ResultSet executSqlProcedure1(Connection connection, String sql, Object... arg) throws DaoException {

		CallableStatement prepare;
		ResultSet result = null;
		try {
			connection.setAutoCommit(false);
			prepare = connection.prepareCall(sql);
			setSqlPara(prepare, arg);
			result = prepare.getResultSet();

		} catch (SQLException e) {
			// e.printStackTrace();
			throw new DaoException(DaoException.DATAEXCEPTION, "数据异常");

		}

		return result;
	}

	// public CallableStatement executSqlProcedureOut(Connection connection,
	// String sql, Object... arg) throws DataBaseException {
	//
	// CallableStatement prepare;
	// try {
	// connection.setAutoCommit(false);
	// prepare = connection.prepareCall(sql);
	// setSqlPara(prepare, arg);
	// // boolean bol =
	// prepare.execute();
	// // printCallableStatement(prepare, bol) ;
	// } catch (SQLException e) {
	// // e.printStackTrace();
	// throw new DataBaseException(DataBaseException.DATAEXCEPTION, "数据异常");
	//
	// }
	//
	// return prepare;
	// }

	public static CallableStatement executSqlProcedureMoreSet(Connection connection, String sql, Object... arg) throws DaoException {

		CallableStatement prepare;
		try {
			connection.setAutoCommit(false);
			prepare = connection.prepareCall(sql);
			setSqlPara(prepare, arg);
			// boolean bol =
			prepare.execute();
			// printCallableStatement(prepare, bol) ;

		} catch (SQLException e) {
			// e.printStackTrace();
			throw new DaoException(DaoException.DATAEXCEPTION, "数据异常");

		}

		return prepare;
	}

	/**
	 * 批量，插入，删除，更新数据
	 * 
	 * @param sql
	 * @param arg
	 * @return
	 * @throws DaoException
	 */
	public static int executSqlUpdateBatch(Connection connection, String sql, Collection<Object[]> batch) throws DaoException {

		PreparedStatement prepare;
		int result = 0;
		try {
			connection.setAutoCommit(false);
			prepare = connection.prepareStatement(sql);
			for (Object[] objArray : batch) {
				setSqlPara(prepare, objArray);
				prepare.addBatch();
			}
			// if (DEBUG)
			// System.out.println("sql = " + sql);
			prepare.executeBatch();

		} catch (SQLException e) {

			e.printStackTrace();
			throw new DaoException(DaoException.DATAEXCEPTION, "数据异常");

		}

		return result;
	}

	// public int executSqlInsert(String table, Map<String, Object> para) throws
	// DataBaseException {
	//
	// Connection con = getConnection();
	// return executSqlInsert(con, table, para);
	// }

	/**
	 * 插入 数据
	 * 
	 * @param sql
	 * @param arg
	 * @return
	 * @throws DaoException
	 */
	public static int executSqlInsert(Connection connection, String table, Map<String, Object> para) throws DaoException {

		if (para.size() == 0)
			return 0;

		StringBuffer sql = new StringBuffer("INSERT INTO ");
		StringBuffer sqlFrag = new StringBuffer("(");

		sql.append(table + " (");

		Object[] obj = new Object[para.size()];

		Iterator<Entry<String, Object>> mapIter = para.entrySet().iterator();
		int i = 0;
		while (mapIter.hasNext()) {
			Entry<String, Object> next = mapIter.next();
			if (i == 0) {
				sqlFrag.append("?");
				sql.append(next.getKey());
			} else {
				sqlFrag.append(",?");
				sql.append("," + next.getKey());
			}
			obj[i] = next.getValue();

			i++;
		}
		sqlFrag.append(")");
		sql.append(")");
		sql.append("VALUES ");
		sql.append(sqlFrag);

		PreparedStatement prepare;
		int result = 0;
		try {

			connection.setAutoCommit(false);
			prepare = connection.prepareStatement(sql.toString());
			setSqlPara(prepare, obj);
			// if (DEBUG)
			// System.out.println("sql = " + sql);
			result = prepare.executeUpdate();

		} catch (SQLException e) {

			// e.printStackTrace();
			throw new DaoException(DaoException.DATAEXCEPTION, "数据异常");
		}

		return result;
	}

	/**
	 * 更新 数据
	 * 
	 * @param sql
	 * @param arg
	 * @return
	 * @throws DaoException
	 */
	public static int executSqlUpdate(Connection connection, String table, Map<String, Object> update, Map<String, Object> where) throws DaoException {

		if (update.size() == 0)
			return 0;

		StringBuffer sql = new StringBuffer("UPDATE " + table + " SET ");

		Object[] obj = new Object[update.size() + where.size()];

		Iterator<Entry<String, Object>> mapIter = update.entrySet().iterator();
		int i = 0;
		// String sql =
		// "UPDATE user SET username = ? , head_icon=? WHERE user_id=?";
		while (mapIter.hasNext()) {
			Entry<String, Object> next = mapIter.next();
			if (i == 0) {
				sql.append(next.getKey() + "=?");
			} else {
				sql.append("," + next.getKey() + "=?");
			}
			obj[i] = next.getValue();

			i++;
		}
		sql.append(" WHERE ");
		Iterator<Entry<String, Object>> whereIter = where.entrySet().iterator();
		// String sql =
		// "UPDATE user SET username = ? , head_icon=? WHERE user_id=?";
		int m = 0;
		while (whereIter.hasNext()) {
			Entry<String, Object> next = whereIter.next();
			if (m == 0) {
				sql.append(next.getKey() + "=?");
			} else {
				sql.append(" AND " + next.getKey() + "=?");
			}
			obj[i] = next.getValue();

			i++;
			m++;
		}

		PreparedStatement prepare;
		int result = 0;
		try {
			connection.setAutoCommit(false);
			prepare = connection.prepareStatement(sql.toString());
			setSqlPara(prepare, obj);
			// if (DEBUG)
			// System.out.println("sql = " + sql);
			result = prepare.executeUpdate();

		} catch (SQLException e) {

			throw new DaoException(DaoException.DATAEXCEPTION, "数据异常");
		}

		return result;
	}

	/**
	 * 查找
	 * 
	 * @param sql
	 * @param arg
	 * @return
	 * @throws DaoException
	 */
	public static ResultSet executSql(Connection connection, String sql, Object... arg) {

		PreparedStatement prepare;
		ResultSet result = null;
		try {
			connection.setAutoCommit(false);
			prepare = connection.prepareStatement(sql);
			setSqlPara(prepare, arg);
			result = prepare.executeQuery();

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return result;
	}

	public static void close(Connection connection) {
		if (connection == null)
			return;
		try {
			// connection.setAutoCommit(false);
			if (!connection.getAutoCommit()) {
				connection.commit();
			}

		} catch (SQLException e) {

			try {
				connection.setAutoCommit(false);
				connection.rollback();
				connection.commit();
				// throw new
				// DataBaseException(DataBaseException.DATAEXCEPTION,
				// "数据异常");
			} catch (SQLException e1) {

				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
			try {
				connection.close();
				stopCount++;
			} catch (SQLException e) {

				e.printStackTrace();
			}
		}
	}

	/**
	 * 向preparedStatement 中添加参数
	 * 
	 * @param prepare
	 * @param arg
	 * @throws SQLException
	 */
	private static void setSqlPara(PreparedStatement prepare, Object... arg) throws SQLException {
		if (arg == null || arg.length == 0 || prepare == null)
			return;
		for (int i = 0; i < arg.length; i++) {

			if (arg[i] == null) {
				prepare.setString(i + 1, null);
			} else if (arg[i] instanceof Integer)
				prepare.setInt(i + 1, (int) arg[i]);
			else if (arg[i] instanceof String) {
				prepare.setString(i + 1, (String) arg[i]);
			} else if (arg[i] instanceof Long) {
				prepare.setLong(i + 1, (Long) arg[i]);
			} else if (arg[i] instanceof Double) {
				prepare.setDouble(i + 1, (Double) arg[i]);
			} else if (arg[i] instanceof Float) {
				prepare.setFloat(i + 1, (Float) arg[i]);
			} else if (arg[i] instanceof Time)
				prepare.setTime(i + 1, (Time) arg[i]);
			else if (arg[i] instanceof Timestamp)
				prepare.setTimestamp(i + 1, (Timestamp) arg[i]);
			else if (arg[i] instanceof Byte)
				prepare.setByte(i + 1, (Byte) arg[i]);
			else {
				prepare.setObject(i + 1, arg[i]);
			}
		}
	}

	/**
	 * 向preparedStatement 中添加参数
	 * 
	 * @param prepare
	 * @param arg
	 * @throws SQLException
	 */
	private static void setCallPara(CallableStatement prepare, Object... arg) throws SQLException {
		if (arg == null || arg.length == 0 || prepare == null)
			return;
		for (int i = 0; i < arg.length; i++) {

			if (arg[i] == null)
				prepare.setString(i + 1, null);
			else if (arg[i] instanceof Integer)
				prepare.setInt(i + 1, (int) arg[i]);
			else if (arg[i] instanceof String) {
				prepare.setString(i + 1, (String) arg[i]);
			} else if (arg[i] instanceof Long) {
				prepare.setLong(i + 1, (Long) arg[i]);
			} else if (arg[i] instanceof Timestamp)
				prepare.setTimestamp(i + 1, (Timestamp) arg[i]);
			else if (arg[i] instanceof Time)
				prepare.setTime(i + 1, (Time) arg[i]);
			else if (arg[i] instanceof Byte)
				prepare.setByte(i + 1, (Byte) arg[i]);
			else if (arg[i] instanceof Array)
				prepare.setArray(i + 1, ((Array) arg[i]));
			else if (arg[i] instanceof CallRegisterOut) {
				CallRegisterOut call = (CallRegisterOut) arg[i];
				if (StringUtils.isNullOrEmpty(call.parameterName))
					prepare.registerOutParameter(i + 1, call.type);
				else
					prepare.registerOutParameter(call.parameterName, call.type);
			} else {
				prepare.setObject(i + 1, arg[i]);
			}
		}
	}

}
