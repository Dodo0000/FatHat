package org.fathat.util;

import java.sql.Connection;
import java.sql.SQLException;

import org.fathat.pool.ConnectionPoolManager;

/**
 * @author wyhong
 * @date 2018-4-28
 */
public class ConnectionUtil {

//	public static Connection getConnection(DataSource datasource) {
//		try {
//			Class.forName(datasource.getDriverClassName());
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}
//		Connection connection = null;
//		try {
//			connection = (Connection) DriverManager.getConnection(datasource.getUrl(), datasource.getUsername(), datasource.getPassword());
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		if(connection == null){
//			System.out.println("Encountered problems while fetching a jdbc connection.");
//		}
//		return connection;
//	}
private static ConnectionPoolManager connManager = new ConnectionPoolManager("db.properties");
	
	public static Connection getConnection(){
		return connManager.getConnection();
	}
	
	public static void returnConnection(Connection conn){
		connManager.returnConnection(conn);
	}
	
	public static void release(Connection connection){
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
