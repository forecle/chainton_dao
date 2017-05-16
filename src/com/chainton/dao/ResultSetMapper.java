package com.chainton.dao;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.function.BiConsumer;

import javax.persistence.Column;

import org.apache.commons.beanutils.BeanUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class ResultSetMapper {

	public static <T> List<T> mapRersultSetToObject(ResultSet rs, Class<T> outputClass) {

		Vector<String> vector = new Vector<String>();
		try {
			ResultSetMetaData metaData = rs.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				vector.add(metaData.getColumnLabel(i));
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		List<T> outputList = null;
		try {
			if (rs != null) {
				Field[] fields = outputClass.getDeclaredFields();
				while (rs.next()) {
					T bean = (T) outputClass.newInstance();
					for (Field field : fields) {
						if (field.isAnnotationPresent(Column.class)) {
							Column column = field.getAnnotation(Column.class);
							try {

								if (vector.contains(column.name())) {
									if (field.getType() == JSONObject.class) {
										field.set(bean, JSONObject.parseObject(rs.getString(column.name())));
									} else if (field.getType() == JSONArray.class) {
										field.set(bean, JSONArray.parseArray(rs.getString(column.name())));
									} else {
										try {
											Object obj = rs.getObject(column.name());
											BeanUtils.setProperty(bean, field.getName(), obj);
										} catch (Exception e) {

										}
									}
								}

							} catch (Exception e) {

							}

						}

					}
					if (outputList == null) {
						outputList = new ArrayList<T>();
					}
					outputList.add(bean);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return outputList;
	}

	public static Map<String, String> mapRersultSetToMap(ResultSet result) {
		Map<String, String> map = new LinkedHashMap<>();
		try {
			ResultSetMetaData metaData = result.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				map.put(metaData.getColumnLabel(i), result.getString(i));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return map;
	}

	public static JSONObject mapRersultSetToJsonObject(ResultSet result) {
		JSONObject jsonobject = new JSONObject();

		try {
			ResultSetMetaData metaData = result.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				jsonobject.put(metaData.getColumnLabel(i), result.getString(i));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return jsonobject;
	}

	public static List<Map<String, String>> mapRersultSetToListMap(ResultSet result) {
		Vector<String> vector = new Vector<String>();
		List<Map<String, String>> listMap = new ArrayList<>();
		ResultSetMetaData metaData = null;
		try {
			metaData = result.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				vector.add(metaData.getColumnLabel(i));
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		try {
			while (result.next()) {
				Map<String, String> map = new LinkedHashMap<>();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					map.put(vector.get(i - 1), result.getString(i));
				}
				listMap.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return listMap;
	}

	public static <U> U mapRersultSetToObjectOneLine(ResultSet rs, Class<U> outputClass) {
		Vector<String> vector = new Vector<String>();
		try {
			ResultSetMetaData metaData = rs.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				vector.add(metaData.getColumnLabel(i));
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		try {
			if (rs != null) {
				Field[] fields = outputClass.getFields();
				U bean = (U) outputClass.newInstance();
				for (Field field : fields) {
					if (field.isAnnotationPresent(Column.class)) {
						Column column = field.getAnnotation(Column.class);
						if (vector.contains(column.name())) {
							try {
								// if (vector.contains(column.name())) {
								// if (field.getType() == String.class) {
								// field.set(bean, rs.getString(column.name()));
								// } else if (field.getType() == Integer.class)
								// {
								// field.set(bean, rs.getInt(column.name()));
								// } else if (field.getType() == Long.class) {
								// field.set(bean, rs.getLong(column.name()));
								// } else if (field.getType() == Float.class) {
								// field.set(bean, rs.getFloat(column.name()));
								// } else if (field.getType() == Double.class) {
								// field.set(bean, rs.getDouble(column.name()));
								// } else if (field.getType() == Boolean.class)
								// {
								// field.set(bean,
								// rs.getBoolean(column.name()));
								// } else if (field.getType() ==
								// JSONObject.class) {
								// field.set(bean,
								// JSONObject.parseObject(rs.getString(column.name())));
								// } else if (field.getType() ==
								// JSONArray.class) {
								// field.set(bean,
								// JSONArray.parseArray(rs.getString(column.name())));
								// } else if (field.getType() ==
								// Timestamp.class) {
								// field.set(bean,
								// rs.getTimestamp(column.name()));
								// } else if (field.getType() == Map.class) {
								// field.set(bean, (Map<?, ?>)
								// JSONObject.parse(column.name()));
								// } else if (field.getType() == Short.class) {
								// field.set(bean, rs.getShort(column.name()));
								// } else if (field.getType() == Time.class) {
								// field.set(bean, rs.getTime(column.name()));
								// } else if (field.getType() == Date.class) {
								// field.set(bean, rs.getDate(column.name()));
								// } else if (field.getType() == Byte.class) {
								// field.set(bean, rs.getByte(column.name()));
								// } else {
								// BeanUtils.setProperty(bean, field.getName(),
								// rs.getObject(column.name()));
								// }
								// }
								if (vector.contains(column.name())) {
									if (field.getType() == JSONObject.class) {
										field.set(bean, JSONObject.parseObject(rs.getString(column.name())));
									} else if (field.getType() == JSONArray.class) {
										field.set(bean, JSONArray.parseArray(rs.getString(column.name())));
									} else {
										try {
											Object obj = rs.getObject(column.name());
											BeanUtils.setProperty(bean, field.getName(), obj);
										} catch (Exception e) {

										}
									}
								}
							} catch (Exception e) {

							}
						}
					}
				}
				return bean;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static <T> List<T> mapRersultSetToObjectOneLine(JSONArray jsonArray, Class<T> classes) {

		Map<String, String> fMap = new HashMap<>();
		Field[] fields = classes.getFields();
		for (Field field : fields) {
			Column ano = field.getAnnotation(Column.class);
			if (ano != null) {
				fMap.put(ano.name(), field.getName());
			}
		}

		if (jsonArray == null)
			return null;
		List<T> list = new ArrayList<>();
		try {
			for (int i = 0; i < jsonArray.size(); i++) {
				T bean = (T) classes.newInstance();
				JSONObject ob = (JSONObject) jsonArray.get(i);
				ob.forEach(new BiConsumer<String, Object>() {

					@Override
					public void accept(String t, Object u) {
						if (fMap.containsKey(t)) {
							try {
								BeanUtils.setProperty(bean, fMap.get(t), u);
							} catch (IllegalAccessException | InvocationTargetException e) {
								e.printStackTrace();
							}
						}
					}
				});
				// Iterator<String> iter = fMap.keySet().iterator();
				// while (iter.hasNext()) {
				// String next = iter.next();
				// if (ob.containsKey(next)) {
				//
				// }
				// }
				list.add(bean);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return list;
	}

}