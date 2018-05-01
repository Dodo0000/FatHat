package org.fathat.pool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.fathat.datasource.DataSource;
import org.fathat.util.ConnectionUtil;
import org.fathat.util.PropertiesUtil;

import com.mysql.jdbc.Connection;

/**
 * @author wyhong
 * @date 2018-4-29
 */
public class ConnectionPool {

	private static BlockingQueue<Connection> connectionQueue;
	private final static int INITIAL_SIZE;
	
	static{
		INITIAL_SIZE = Integer.parseInt(PropertiesUtil.getProperties("db.properties").getProperty("jdbc.initialSize"));
	}

	public static Connection getConnection() {
		if(connectionQueue == null){
			connectionQueue = new ArrayBlockingQueue<Connection>(INITIAL_SIZE);
			DataSource dataSource = new DataSource("db.properties");
			for (int i = 0; i < INITIAL_SIZE; i++) {
				try {
					connectionQueue.put(ConnectionUtil.getConnection(dataSource));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		try {
			return connectionQueue.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void returnConnection(Connection connection){
		try {
			connectionQueue.put(connection);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
