package org.fathat.proxy;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fathat.annotation.Sql;
import org.fathat.exception.InsertException;
import org.fathat.exception.UnsupportReturnType;
import org.fathat.exception.returnTypeMismatchedException;
import org.fathat.pool.ConnectionPool;

import com.mysql.jdbc.Connection;

/*
 * @author wyhong
 * @date 2018-4-28
 */
public class DaoProxy implements InvocationHandler {

	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		// 检查注解
		if (method.isAnnotationPresent(Sql.class)) {
			Sql annotation = method.getAnnotation(Sql.class);
			String sql = annotation.value();
			boolean isSelect = "select".equalsIgnoreCase(sql.split(" ")[0]);
			if (isSelect) {
				return handleSelect(sql, method, args);
			} else {
				// 返回影响的行数
				return handleUpdate(sql, method, args);
			}

		}
		return null;
	}

	private Integer handleUpdate(String sql, Method method, Object[] args)
			throws SQLException, InsertException {
		// 以后从pool获取
		Connection connection = ConnectionPool.getConnection();
		// 普通insert是指直接插入对应的VALUE(?,?,?) 带#号开头的是插入对象的情况
		/** 约定#insert是插入一个对象,而且对象的属性都是基本类型 */
		if ("#insert".equalsIgnoreCase(sql.split("\\s+")[0])) {
			String actualSql = sql.substring(1);
			PreparedStatement prepareStatement = connection
					.prepareStatement(actualSql);
			if (args.length > 1) {
				throw new InsertException(
						"#insert : more than one object param");
			}
			// 获取record中所有的Field, 并且建立一个Map<String, Object>
			Object record = args[0];
			Field[] fields = record.getClass().getDeclaredFields();
			Map<String, Object> fieldMap = new HashMap<String, Object>();
			for (Field field : fields) {
				// 设置为可获取
				field.setAccessible(true);
				String fieldName = field.getName();
				Object fieldValue = null;
				// 字段名
				System.out.print(fieldName + ":");
				if (field.getType().getName()
						.equals(java.lang.String.class.getName())) {
					// String type
					try {
						fieldValue = field.get(record);
						System.out.println(fieldValue);
					} catch (IllegalArgumentException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (field.getType().getName()
						.equals(java.lang.Integer.class.getName())
						|| field.getType().getName().equals("int")) {
					// Integer type
					try {
						fieldValue = field.getInt(record);
						System.out.println(fieldValue);
					} catch (IllegalArgumentException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				// ...其他类型还要继续写下
				else {
					throw new InsertException(
							"#insert : only basic types allowed");
				}
				fieldMap.put(fieldName, fieldValue);
			}

			// 根据fieldMap设置prepareStatement中的参数
			String tableName = actualSql.split("\\s+")[2];
			setParam(prepareStatement, fieldMap, tableName);
			System.out.println(prepareStatement.toString());
			Integer updatedRows = prepareStatement.executeUpdate();
			System.out.println("update:" + updatedRows);
			return updatedRows;
		} else {
			// 约定sql开头不能有空格
			PreparedStatement prepareStatement = connection
					.prepareStatement(sql);
			setParam(prepareStatement, method, args);
			System.out.println(prepareStatement.toString());
			Integer updatedRows = prepareStatement.executeUpdate();
			System.out.println("update:" + updatedRows);
			return updatedRows;
		}
	}

	// 插入一个对象时需要
	private void setParam(PreparedStatement prepareStatement,
			Map<String, Object> fieldMap, String tableName) throws SQLException {
		String[] columnNames = getColumnNames(tableName);
		for(int i=1; i<columnNames.length; i++){
			String columnName = columnNames[i];
			System.out.println("columnName:"+columnName);
			if(fieldMap.containsKey(columnName)){
				Object fieldValue = fieldMap.get(columnName);
				System.out.println("fieldValue:"+fieldValue);
				System.out.println("fieldValue.getClass():"+fieldValue.getClass());
				if (int.class == fieldValue.getClass() || Integer.class == fieldValue.getClass()) {
					prepareStatement.setInt(i, (int) fieldValue);
				} else if (short.class == fieldValue.getClass() || Short.class == fieldValue.getClass()) {
					prepareStatement.setShort(i, (short) fieldValue);
				} else if (long.class == fieldValue.getClass() || Long.class == fieldValue.getClass()) {
					prepareStatement.setLong(i, (long) fieldValue);
				} else if (float.class == fieldValue.getClass() || Float.class == fieldValue.getClass()) {
					prepareStatement.setFloat(i, (float) fieldValue);
				} else if (double.class == fieldValue.getClass() || Double.class == fieldValue.getClass()) {
					prepareStatement.setDouble(i, (double) fieldValue);
				} else if (boolean.class == fieldValue.getClass() || Boolean.class == fieldValue.getClass()) {
					prepareStatement.setBoolean(i, (boolean) fieldValue);
				} else if (Date.class == fieldValue.getClass()) {
					prepareStatement.setDate(i, (Date) fieldValue);
				} else if (Timestamp.class == fieldValue.getClass()) {
					prepareStatement.setTimestamp(i, (Timestamp) fieldValue);
				} else {
					prepareStatement.setString(i, (String)fieldValue);
				}
			}
		}
	}

	// 根据表明获取相应列的名称
	private String[] getColumnNames(String tableName) {
		String[] columnNames = null;
		Connection conn = ConnectionPool.getConnection();
		String sql = "select * from " + tableName + " limit 1";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery(sql);
			ResultSetMetaData metadata = rs.getMetaData();
			columnNames = new String[metadata.getColumnCount()+1];
			for (int i = 1; i <= metadata.getColumnCount(); i++) {
				columnNames[i] = metadata.getColumnName(i);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return columnNames;
	}

	// 异常要不优化一下？？直接Exception算了
	private Object handleSelect(String sql, Method method, Object[] args)
			throws Exception {
		// 以后从pool获取
		Connection connection = ConnectionPool.getConnection();
		PreparedStatement prepareStatement = connection.prepareStatement(sql);
		// 约定sql开头不能有空格
		setParam(prepareStatement, method, args);
		System.out.println(prepareStatement.toString());
		ResultSet rs = prepareStatement.executeQuery();
		System.out.println("warnings:" + prepareStatement.getWarnings());
		// 获取返回值类型
		Class returnType = (Class) method.getReturnType();
		List list = null;
		if (returnType == List.class) {
			ParameterizedType genericReturnType = (ParameterizedType) method
					.getGenericReturnType();
			returnType = (Class) genericReturnType.getActualTypeArguments()[0];
			list = new ArrayList();
		}
		if (list == null && getRowCount(rs) > 1) {
			throw new returnTypeMismatchedException(
					"return type is a single model but result set has more than 1 row");
		}
		ResultSetMetaData metaData = rs.getMetaData();
		Object instance = null;
		if (isBasicType(returnType)) {
			// 返回基本数据类型
			while (rs.next()) {
				instance = constructBasicObject(returnType, rs);
				if (list != null) {
					list.add(instance);
				} else {
					break;
				}
			}
		} else {
			// 返回bean
			Method[] methods = returnType.getMethods();
			Map<String, Method> setters = new HashMap<String, Method>();
			fillSetters(setters, methods);
			while (rs.next()) {
				instance = returnType.newInstance();
				setAttributes(instance, rs, setters, metaData);
				if (list != null) {
					list.add(instance);
				} else {
					break;
				}
			}
		}
		rs.close();
		prepareStatement.close();
		ConnectionPool.returnConnection(connection);
		if (list != null)
			return list;
		return instance;
	}

	private Object constructBasicObject(Class returnType, ResultSet rs)
			throws Exception {
		Object instance = null;
		Constructor constructor = null;
		if (Integer.class == returnType) {
			constructor = returnType.getDeclaredConstructor(int.class);
			instance = constructor.newInstance(rs.getInt(1));
		} else if (Double.class == returnType) {
			constructor = returnType.getDeclaredConstructor(double.class);
			instance = constructor.newInstance(rs.getDouble(1));
		} else if (Long.class == returnType) {
			constructor = returnType.getDeclaredConstructor(long.class);
			instance = constructor.newInstance(rs.getLong(1));
		} else if (Date.class == returnType) {
			constructor = returnType.getDeclaredConstructor(long.class);
			instance = constructor.newInstance(rs.getLong(1));
		} else if (String.class == returnType) {
			constructor = returnType.getDeclaredConstructor(String.class);
			instance = constructor.newInstance(rs.getString(1));
		} else if (Boolean.class == returnType) {
			constructor = returnType.getDeclaredConstructor(boolean.class);
			instance = constructor.newInstance(rs.getBoolean(1));
		} else {
			throw new UnsupportReturnType(returnType + " is unsupport!");
		}
		return instance;
	}

	private boolean isBasicType(Class returnType) {
		return Short.class == returnType || Integer.class == returnType
				|| Float.class == returnType || Double.class == returnType
				|| Long.class == returnType || Character.class == returnType
				|| String.class == returnType || Boolean.class == returnType
				|| Date.class == returnType;
	}

	private int getRowCount(ResultSet rs) throws SQLException {
		int rows = 0;
		rs.last();
		rows = rs.getRow();
		rs.beforeFirst();
		return rows;
	}

	private void setAttributes(Object instance, ResultSet rs,
			Map<String, Method> setters, ResultSetMetaData metaData)
			throws SQLException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		for (int i = 1; i <= metaData.getColumnCount(); i++) {
			// System.out.println("column name:"+metaData.getColumnName(i));
			Method target = setters.get(metaData.getColumnName(i));
			Class<?>[] paramClass = target.getParameterTypes();
			// System.out.println(paramClass[0].getSimpleName());
			if (short.class == paramClass[0] || int.class == paramClass[0]) {
				target.invoke(instance, rs.getInt(i));
			} else if (long.class == paramClass[0]) {
				target.invoke(instance, rs.getLong(i));
			} else if (Date.class == paramClass[0]) {
				target.invoke(instance, rs.getDate(i));
			} else if (boolean.class == paramClass[0]) {
				target.invoke(instance, (boolean) rs.getBoolean(i));
			} else if (float.class == paramClass[0]
					|| double.class == paramClass[0]) {
				target.invoke(instance, (double) rs.getDouble(i));
			} else {
				target.invoke(instance, (String) rs.getString(i));
			}
		}
	}

	private void fillSetters(Map<String, Method> setters, Method[] methods) {
		// 获取所有setter
		for (Method _method : methods) {
			String methodName = _method.getName();
			if (!methodName.startsWith("set"))
				continue;
			String fieldName = methodName.substring(3);
			fieldName = (char) ((int) fieldName.charAt(0) + 32)
					+ fieldName.substring(1);
			setters.put(fieldName, _method);
		}
	}

	private void setParam(PreparedStatement prepareStatement, Method method,
			Object[] args) throws SQLException {
		// 获取方法参数
		Class<?>[] parameterTypes = method.getParameterTypes();
		// 设置sql（方法参数顺序须按声明顺序写）
		for (int i = 0; i < parameterTypes.length; i++) {
			if (int.class == parameterTypes[i]) {
				prepareStatement.setInt(i + 1, (int) args[i]);
			} else if (short.class == parameterTypes[i]) {
				prepareStatement.setShort(i + 1, (short) args[i]);
			} else if (long.class == parameterTypes[i]) {
				prepareStatement.setLong(i + 1, (long) args[i]);
			} else if (float.class == parameterTypes[i]) {
				prepareStatement.setFloat(i + 1, (float) args[i]);
			} else if (double.class == parameterTypes[i]) {
				prepareStatement.setDouble(i + 1, (double) args[i]);
			} else if (boolean.class == parameterTypes[i]) {
				prepareStatement.setBoolean(i + 1, (boolean) args[i]);
			} else if (Date.class == parameterTypes[i]) {
				prepareStatement.setDate(i + 1, (Date) args[i]);
			} else if (Timestamp.class == parameterTypes[i]) {
				prepareStatement.setTimestamp(i + 1, (Timestamp) args[i]);
			} else {
				prepareStatement.setString(i + 1, (String) args[i]);
			}
		}
	}

}
