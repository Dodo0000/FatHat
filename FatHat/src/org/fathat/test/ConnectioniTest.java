package org.fathat.test;

import java.sql.PreparedStatement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.fathat.datasource.DataSource;
import org.fathat.util.ConnectionUtil;
import org.junit.Test;


/**
 * @author wyhong
 * @date 2018-4-28
 */
public class ConnectioniTest {

	@Test
	public void test(){
		Connection connection = ConnectionUtil.getConnection();
		PreparedStatement prepareStatement = null;
		try {
			 prepareStatement = connection.prepareStatement("select * from enterprise where id > ?");
			 prepareStatement.setInt(1, 1);
			 System.out.println(prepareStatement.toString());
			 ResultSet resultSet = prepareStatement.executeQuery();
			 
			 //将cursor移动到最后，移动的过程中才会计数
			 resultSet.last();
			 System.out.println(resultSet.getRow());
			 //返回开头，不然指针在末尾无法取值
			 resultSet.beforeFirst();
			 
			 while(resultSet.next()){
				 System.out.print(resultSet.getInt(1)+" ");
				 System.out.print(resultSet.getString(2)+" ");
				 System.out.print(resultSet.getString(3)+" ");
				 System.out.print(resultSet.getString(4)+" ");
				 System.out.println();
			 }
			 resultSet.close();
			 prepareStatement.close();
			 ConnectionUtil.release(connection);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}
