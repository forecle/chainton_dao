package com.chainton.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * @author Administrator
 *
 */
public interface BaseDao {

	public default ResultSet executSql(Connection connection, String sql, Object... arg) {
		return DatabaseConnection.executSql(connection, sql, arg);
	}

	public default <T> T executSql(Class<T> clases, T defau, Connection connection, String sql, Object... arg) {
		ResultSet result = DatabaseConnection.executSql(connection, sql, arg);
		return nextParaData(result, clases, defau);
	}

	public default <T> T executSql(Class<T> clases, T defau, String sql, Object... arg) {
		Connection connection = DatabaseConnection.getConnection();
		try {
			ResultSet result = DatabaseConnection.executSql(connection, sql, arg);
			return nextParaData(result, clases, defau);
		} catch (Exception e) {
		} finally {
			DatabaseConnection.close(connection);
		}
		return defau;
	}

	public default <T> T executeCallOut(Class<T> clases, T defau, Connection connection, String sql, Object... arg) {
		return nextParaData(DatabaseConnection.executeCallOut(connection, sql, arg), clases, defau);
	}

	public default <T> T executeCallOut(Class<T> clases, T defau, String sql, Object... arg) {

		Connection connection = DatabaseConnection.getConnection();
		try {
			return nextParaData(DatabaseConnection.executeCallOut(connection, sql, arg), clases, defau);
		} catch (Exception e) {
		} finally {
			DatabaseConnection.close(connection);
		}
		return defau;
	}

	public default ResultSet executeCallOut(Connection connection, String sql, Object... arg) throws DaoException {
		return DatabaseConnection.executeCallOut(connection, sql, arg);
	}

	public default <T> T nextParaData(ResultSet result, Class<T> clases, T defau) {

		if (result == null)
			return (T) defau;
		try {
			if (result.next()) {
				return paraData(result, clases, defau);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return (T) defau;
	}

	@SuppressWarnings("unchecked")
	public default <T> T paraData(ResultSet result, Class<T> clases, T defau) {

		if (result == null)
			return (T) defau;

		try {
			if (String.class.isAssignableFrom(clases)) {
				return (T) result.getString(1);
			} else if (Integer.class.isAssignableFrom(clases) || int.class.isAssignableFrom(clases)) {
				return (T) (Integer) result.getInt(1);
			} else if (Long.class.isAssignableFrom(clases) || long.class.isAssignableFrom(clases)) {
				return (T) (Long) result.getLong(1);
			} else if (Float.class.isAssignableFrom(clases) || float.class.isAssignableFrom(clases)) {
				return (T) (Float) result.getFloat(1);
			} else if (Double.class.isAssignableFrom(clases) || double.class.isAssignableFrom(clases)) {
				return (T) (Double) result.getDouble(1);
			} else if (Boolean.class.isAssignableFrom(clases) || boolean.class.isAssignableFrom(clases)) {
				return (T) (Boolean) result.getBoolean(1);
			} else if (JSONObject.class.isAssignableFrom(clases)) {
				if (result.getMetaData().getColumnCount() == 1) {
					return (T) JSONObject.parseObject(result.getString(1));
				} else {
					return (T) ResultSetMapper.mapRersultSetToJsonObject(result);
				}
			} else if (JSONArray.class.isAssignableFrom(clases)) {
				return (T) JSONArray.parseArray(result.getString(1));
			} else if (Timestamp.class.isAssignableFrom(clases)) {
				return (T) (Timestamp) result.getTimestamp(1);
			} else if (Map.class.isAssignableFrom(clases)) {
				return (T) (Map<String, String>) ResultSetMapper.mapRersultSetToMap(result);
			} else
				return ResultSetMapper.mapRersultSetToObjectOneLine(result, clases);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return (T) defau;
	}

	public default <T> List<T> executSqlList(Class<T> clases, Connection connection, String sql, Object... arg) {
		ResultSet result = null;
		result = DatabaseConnection.executSql(connection, sql, arg);
		List<T> l = new ArrayList<>();
		try {
			while (result.next()) {
				T t = paraData(result, clases, null);
				if (t != null)
					l.add(t);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return l;
	}

	public default <T> List<T> executSqlList(Class<T> clases, String sql, Object... arg) {
		Connection connection = DatabaseConnection.getConnection();
		try {
			return executSqlList(clases, connection, sql, arg);
		} catch (Exception e) {
		} finally {
			DatabaseConnection.close(connection);
		}
		return null;
	}

	public default int executSqlUpdate(Connection connection, String table, Map<String, Object> update, Map<String, Object> where) throws DaoException {
		return DatabaseConnection.executSqlUpdate(connection, table, update, where);
	}

	public default int executSqlUpdate(String table, Map<String, Object> update, Map<String, Object> where) throws DaoException {
		Connection connection = DatabaseConnection.getConnection();
		try {
			return executSqlUpdate(table, update, where);
		} catch (Exception e) {
		} finally {
			DatabaseConnection.close(connection);
		}
		return 0;
	}

	public default int executSqlInsert(Connection connection, String table, Map<String, Object> para) throws DaoException {
		return DatabaseConnection.executSqlInsert(connection, table, para);
	}

	public default int executSqlInsert(String table, Map<String, Object> para) throws DaoException {
		Connection connection = DatabaseConnection.getConnection();
		try {
			return executSqlInsert(connection, table, para);
		} catch (Exception e) {
		} finally {
			DatabaseConnection.close(connection);
		}
		return 0;
	}

	public default int executSqlUpdateBatch(Connection connection, String sql, Collection<Object[]> batch) throws DaoException {
		return DatabaseConnection.executSqlUpdateBatch(connection, sql, batch);
	}

	public default int executSqlUpdateBatch(String sql, Collection<Object[]> batch) throws DaoException {
		Connection connection = DatabaseConnection.getConnection();
		try {
			return executSqlUpdateBatch(connection, sql, batch);
		} catch (Exception e) {
		} finally {
			DatabaseConnection.close(connection);
		}
		return 0;
	}

	public default CallableStatement executSqlProcedureMoreSet(Connection connection, String sql, Object... arg) throws DaoException {
		return DatabaseConnection.executSqlProcedureMoreSet(connection, sql, arg);
	}

	public default ResultSet executSqlProcedure1(Connection connection, String sql, Object... arg) throws DaoException {
		return DatabaseConnection.executSqlProcedure1(connection, sql, arg);
	}

	public default <T> T executSqlProcedureByAutocommit(Connection connection, Class<T> clases, T defau, Boolean autoCommit, String sql, Object... arg) {
		return DatabaseConnection.executSqlProcedureByAutocommit(connection, clases, defau, autoCommit, sql, arg);
	}

	public default <T> T executSqlProcedureByAutocommit(Class<T> clases, T defau, Boolean autoCommit, String sql, Object... arg) {
		Connection connection = DatabaseConnection.getConnection();
		try {
			return executSqlProcedureByAutocommit(clases, defau, autoCommit, sql, arg);
		} catch (Exception e) {
		} finally {
			DatabaseConnection.close(connection);
		}
		return defau;
	}

	public default <T> List<T> executSqlProcedureByAutocommitResultList(Connection connection, Class<T> clases, Boolean autoCommit, String sql, Object... arg) {
		return DatabaseConnection.executSqlProcedureByAutocommitResultList(connection, clases, autoCommit, sql, arg);
	}

	public default <T> List<T> executSqlProcedureByAutocommitResultList(Class<T> clases, Boolean autoCommit, String sql, Object... arg) {
		Connection connection = DatabaseConnection.getConnection();
		try {
			return executSqlProcedureByAutocommitResultList(connection, clases, autoCommit, sql, arg);
		} catch (Exception e) {
		} finally {
			DatabaseConnection.close(connection);
		}
		return null;
	}

	public default ResultSet executSqlProcedureByAutocommit(Connection connection, Boolean autoCommit, String sql, Object... arg) {
		return DatabaseConnection.executSqlProcedureByAutocommit(connection, autoCommit, sql, arg);
	}

	public default ResultSet executSqlProcedure(Connection connection, String sql, Object... arg) throws DaoException {
		return DatabaseConnection.executSqlProcedure(connection, sql, arg);
	}

	public default int executSqlUpdate(Connection connection, String sql, Object... arg) throws DaoException {
		return DatabaseConnection.executSqlUpdate(connection, sql, arg);
	}

	public default int executSqlUpdate(String sql, Object... arg) throws DaoException {
		Connection connection = DatabaseConnection.getConnection();
		try {
			return executSqlUpdate(connection, sql, arg);
		} finally {
			DatabaseConnection.close(connection);
		}
	}

	public default ResultSet executSqlProcedure(Connection connection, boolean autocommiet, String sql, Object... arg) throws DaoException {
		return DatabaseConnection.executSqlProcedure(connection, autocommiet, sql, arg);
	}

	public default int executSqlUpdate(Connection connection, boolean autoCommit, String sql, Object... arg) throws DaoException {
		return DatabaseConnection.executSqlUpdate(connection, autoCommit, sql, arg);
	}

	public default int executSqlUpdate(boolean autoCommit, String sql, Object... arg) throws DaoException {
		Connection connection = DatabaseConnection.getConnection();
		try {
			return executSqlUpdate(connection, autoCommit, sql, arg);
		} catch (Exception e) {
		} finally {
			DatabaseConnection.close(connection);
		}
		return 0;
	}
}
