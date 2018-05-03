package org.fathat.pool;

import java.sql.Connection;  
import java.sql.DatabaseMetaData;  
import java.sql.Driver;  
import java.sql.DriverManager;  
import java.sql.SQLException;  
import java.sql.Statement;    
import java.util.List;
import java.util.Properties;
import java.util.Vector;  

import org.fathat.datasource.DataSource;
import org.fathat.util.PropertiesUtil;
  
public class ConnectionPool {  
	//与当前连接池相关的数据源
    private DataSource dataSource = null;
    // 测试连接是否可用的测试表名，默认没有测试表
    private String testTable = ""; 
    // 连接池的初始大小
    private int initialConnections = 10;
    // 连接池自动增加的大小
    private int incrementalConnections = 5;
    // 连接池最大的大小
    private int maxConnections = 50;
    //空闲连接  
    private List<Connection> freeConnections = null;  
    //忙的连接  
    private List<Connection> busyConnections = null; 
    //忙的连接的总数
    private int numOfBusyConnection = 0;
    //连接池是否已经初始化
    private boolean isCreated = false;
    // 它中存放的对象为 PooledConnection 型  
    
    /**
     * 指定当前连接池跟哪个dataSouce有关
     * @param dataSource
     */
    public ConnectionPool(DataSource dataSource, Properties properties) {  
        this.dataSource = dataSource;
        if(properties.getProperty("jdbc.initialConnections")!=null){
        	this.incrementalConnections = Integer.parseInt(properties.getProperty("jdbc.initialConnections"));
        }
        if(properties.getProperty("jdbc.incrementalConnections")!=null){
        	this.incrementalConnections = Integer.parseInt(properties.getProperty("jdbc.incrementalConnections"));
        }
		if(properties.getProperty("jdbc.maxConnections")!=null){
			this.maxConnections = Integer.parseInt(properties.getProperty("jdbc.maxConnections"));
		}
    }  
  
    /** 
     * 返回连接池的初始大小 
     *  
     * @return 初始连接池中可获得的连接数量 
     */  
    public int getInitialConnections() {  
        return this.initialConnections;  
    }  
    /** 
     * 设置连接池的初始大小 
     *  
     * @param 用于设置初始连接池中连接的数量 
     */  
    public void setInitialConnections(int initialConnections) {  
        this.initialConnections = initialConnections;  
    }  
    /** 
     * 返回连接池自动增加的大小 、 
     *  
     * @return 连接池自动增加的大小 
     */  
    public int getIncrementalConnections() {  
        return this.incrementalConnections;  
    }  
    /** 
     * 设置连接池自动增加的大小 
     *  
     * @param 连接池自动增加的大小 
     */  
  
    public void setIncrementalConnections(int incrementalConnections) {  
        this.incrementalConnections = incrementalConnections;  
    }  
    /** 
     * 返回连接池中最大的可用连接数量 
     *  
     * @return 连接池中最大的可用连接数量 
     */  
    public int getMaxConnections() {  
        return this.maxConnections;  
    }  
    /** 
     * 设置连接池中最大可用的连接数量 
     *  
     * @param 设置连接池中最大可用的连接数量值 
     */  
    public void setMaxConnections(int maxConnections) {  
        this.maxConnections = maxConnections;  
    }  
  
    /** 
     * 获取测试数据库表的名字 
     *  
     * @return 测试数据库表的名字 
     */  
  
    public String getTestTable() {  
        return this.testTable;  
    }  
  
    /** 
     * 设置测试表的名字 
     *  
     * @param testTable 
     *            String 测试表的名字 
     */  
  
    public void setTestTable(String testTable) {  
        this.testTable = testTable;  
    }  
  
    /** 
     *  
     * 创建一个数据库连接池，连接池中的可用连接的数量采用类成员 initialConnections 中设置的值 
     */  
    public synchronized void createPool() throws Exception {  
        // 确保连接池没有创建  
        // 如果连接池己经创建了，保存连接的向量 connections 不会为空  
    	if(isCreated){
    		return;
    	}
        // 实例化 JDBC Driver 中指定的驱动类实例  
        Driver driver = (Driver) (Class.forName(this.dataSource.getDriverClassName()).newInstance());  
        DriverManager.registerDriver(driver); // 注册 JDBC 驱动程序  
        //创建空闲队列
        freeConnections = new Vector<Connection>();
        //创建繁忙队列
        busyConnections = new Vector<Connection>();
        // 根据 initialConnections 中设置的值，创建连接。  
        createConnections(this.initialConnections);  
    }  
  
    /** 
     * 创建由 numConnections 指定数目的数据库连接 , 并把这些连接 放入 connections 向量中 
     *  
     * @param numConnections 
     *            要创建的数据库连接的数目 
     */
    private void createConnections(int numOfConnections) throws SQLException {  
        // 循环创建指定数目的数据库连接  
        for (int cur = 0; cur < numOfConnections; cur++) {  
            // 是否连接池中的数据库连接的数量己经达到最大？最大值由类成员 maxConnections  
            // 指出，如果 maxConnections 为 0 或负数，表示连接数量没有限制。  
            // 如果连接数己经达到最大，即退出。  
            if (this.maxConnections > 0  
                    && this.freeConnections.size()+this.busyConnections.size() >= this.maxConnections) {  
                break;  
            }  
            try { 
                //向空闲队列中增加
                freeConnections.add(newConnection());
            } catch (SQLException e) {  
                System.out.println(" 创建数据库连接失败！ " + e.getMessage());  
                throw new SQLException();  
            }  
            // System.out.println(" 数据库连接己创建 ......");  
        }
        //标志成功创建
        this.isCreated = true;
    }  
    /** 
     * 创建一个新的数据库连接并返回它 
     *  
     * @return 返回一个新创建的数据库连接 
     */  
    private Connection newConnection() throws SQLException {  
        // 创建一个数据库连接  
        Connection conn = DriverManager.getConnection(this.dataSource.getUrl(), this.dataSource.getUsername(),  
                this.dataSource.getPassword());  
        // 如果这是第一次创建数据库连接，即检查数据库，获得此数据库允许支持的  
        // 最大客户连接数目  
        if (!isCreated) {  
            DatabaseMetaData metaData = conn.getMetaData();  
            int driverMaxConnections = metaData.getMaxConnections();  
            // 数据库返回的 driverMaxConnections 若为 0 ，表示此数据库没有最大  
            // 连接限制，或数据库的最大连接限制不知道  
            // driverMaxConnections 为返回的一个整数，表示此数据库允许客户连接的数目  
            // 如果连接池中设置的最大连接数量大于数据库允许的连接数目 , 则置连接池的最大  
            // 连接数目为数据库允许的最大数目  
            if (driverMaxConnections > 0  
                    && this.maxConnections > driverMaxConnections) {  
                this.maxConnections = driverMaxConnections;  
            }  
        }  
        return conn; 
    }  
  
    /** 
     * 通过调用 getFreeConnection() 函数返回一个可用的数据库连接 , 如果当前没有可用的数据库连接，并且更多的数据库连接不能创 
     * 建（如连接池大小的限制），此函数等待一会再尝试获取。 
     *  
     * @return 返回一个可用的数据库连接对象 
     */  
    public synchronized Connection getConnection() throws SQLException {  
    	if(!isCreated){
    		return null;
    	}
        // 获得一个可用的数据库连接
        Connection conn = getFreeConnection();   
        // 如果目前没有可以使用的连接，即所有的连接都在使用中  
        while (conn == null) {  
            // 等一会再试  
            // System.out.println("Wait");  
            wait(250);  
            conn = getFreeConnection(); // 重新再试，直到获得可用的连接，如果  
            // getFreeConnection() 返回的为 null  
            // 则表明创建一批连接后也不可获得可用连接  
        }  
        return conn;// 返回获得的可用的连接  
    }  
  
    /** 
     * 本函数从连接池向量 connections 中返回一个可用的的数据库连接，如果 当前没有可用的数据库连接，本函数则根据 
     * incrementalConnections 设置 的值创建几个数据库连接，并放入连接池中。 如果创建后，所有的连接仍都在使用中，则返回 null 
     *  
     * @return 返回一个可用的数据库连接 
     */  
    private Connection getFreeConnection() throws SQLException {  
        // 从连接池中获得一个可用的数据库连接  
        Connection conn = findFreeConnection();  
        if (conn == null) {  
            // 如果目前连接池中没有可用的连接  
            // 创建一些连接  
            createConnections(incrementalConnections);  
            // 重新从池中查找是否有可用连接  
            conn = findFreeConnection();  
            if (conn == null) {  
                // 如果创建连接后仍获得不到可用的连接，则返回 null  
                return null;  
            }  
        }  
        return conn;  
    }  
  
    /** 
     * 查找连接池中所有的连接，查找一个可用的数据库连接， 如果没有可用的连接，返回 null 
     *  
     * @return 返回一个可用的数据库连接 
     */  
    private Connection findFreeConnection() throws SQLException {  
        Connection conn = null;  
        try {  
            // 判断是否超过最大连接数限制  
            if(this.numOfBusyConnection < this.maxConnections){  
                if (freeConnections.size() > 0) {  
                    conn = freeConnections.get(0);  
                    freeConnections.remove(0);  
                } else {
                	//不够那么就去新建
                    conn = newConnection();  
                }  
                  
            }else{  
                //等待5秒继续获得连接,直到从新获得连接  
                wait(5000);  
                conn = getConnection();  
            }  
            if (testConnection(conn)) {  
                busyConnections.add(conn);  
                numOfBusyConnection++;  
            }  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }
        return conn;// 返回找到到的可用连接  
    }  
  
    /** 
     * 测试一个连接是否可用，如果不可用，关掉它并返回 false 否则可用返回 true 
     *  
     * @param conn 
     *            需要测试的数据库连接 
     * @return 返回 true 表示此连接可用， false 表示不可用 
     */  
    private boolean testConnection(Connection conn) {  
        try {  
            // 判断测试表是否存在  
            if (testTable.equals("")) {  
                // 如果测试表为空，试着使用此连接的 setAutoCommit() 方法  
                // 来判断连接否可用（此方法只在部分数据库可用，如果不可用 ,  
                // 抛出异常）。注意：使用测试表的方法更可靠  
                conn.setAutoCommit(true);  
            } else {// 有测试表的时候使用测试表测试  
                //测试当前连接是否可用  
                Statement stmt = conn.createStatement();  
                stmt.execute("select count(*) from " + testTable);  
            }  
        } catch (SQLException e) {  
            // 上面抛出异常，此连接己不可用，关闭它，并返回 false;  
            closeConnection(conn);  
            return false;  
        }  
        // 连接可用，返回 true  
        return true;  
    }  
  
    /** 
     * 此函数返回一个数据库连接到连接池中，并把此连接置为空闲。 所有使用连接池获得的数据库连接均应在不使用此连接时返回它。 
     * 
     * @param 需返回到连接池中的连接对象 
     */  
    public void returnConnection(Connection conn) {  
    	if(!isCreated){
    		System.out.println(" 连接池不存在，无法返回此连接到连接池中 !");  
    		return;
    	}
    	if (testConnection(conn)&& !(freeConnections.size() > this.maxConnections)) {  
            freeConnections.add(conn);  
            busyConnections.remove(conn);  
            numOfBusyConnection--;
        }  
    }  
  
    /** 
     * 刷新连接池中所有的连接对象 
     *  
     */  
  
    public synchronized void refreshConnections() throws SQLException {  
        // 确保连接池己创新存在  
    }  
  
    /** 
     * 关闭连接池中所有的连接，并清空连接池。 
     */  
  
    public synchronized void closeConnectionPool() throws SQLException {  
        // 确保连接池存在，如果不存在，返回   
    	if(!isCreated){
          System.out.println(" 连接池不存在，无法关闭 !");  
          return;
    	}
    	for(int i=0; i<freeConnections.size(); i++){
    		closeConnection(freeConnections.get(i));
    	}
    	freeConnections = null;
    	for(int i=0; i<busyConnections.size(); i++){
    		//等待5秒再关
    		wait(5000);
    		closeConnection(busyConnections.get(i));
    	}
    	busyConnections = null;
    	//还原计数器
    	numOfBusyConnection = 0;
    	//标志为没创建
    	isCreated = false;
    }  
  
    /** 
     * 关闭一个数据库连接 
     *  
     * @param 需要关闭的数据库连接 
     */  
  
    private void closeConnection(Connection conn) {  
        try {  
            conn.close();  
        } catch (SQLException e) {  
            System.out.println(" 关闭数据库连接出错： " + e.getMessage());  
        }  
    }  
    /** 
     * 使程序等待给定的毫秒数 
     *  
     * @param 给定的毫秒数 
     */  
    private void wait(int mSeconds) {  
        try {  
            Thread.sleep(mSeconds);  
        } catch (InterruptedException e) {  
        }  
    }  
    
}  