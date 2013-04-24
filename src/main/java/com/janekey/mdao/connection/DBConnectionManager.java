package com.janekey.mdao.connection;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

/**
 * An instance map multiple pools, a pool map multiple connection.
 * Its implementation with singleton and inner class.
 * One instance has a HashTable object with multiple pools, every pool has a Vector object with connection.
 * User: Janekey(janekey.com)
 * Date: 13-4-22
 * Time: 上午12:10
 * 
 */
public class DBConnectionManager {
	
	private static final Logger LOGGER = Logger.getLogger(DBConnectionManager.class);
	private static final Object LOCK = new Object();
    private static final String LOG_MSG = "[jcms-dbconnection] ";
    private static final int WAIT_TIME = 30;    // default wait time : 30s.

	private static int count = 0;
//	private static int clientLinks;
	private Vector<Driver> drivers = new Vector<Driver>();
	
	private Hashtable<String, DBConnectionPool> pools = new Hashtable<String, DBConnectionPool>();

	private DBConnectionManager() {
		init();
	}

	private static DBConnectionManager instance; // unique instance.

	public static DBConnectionManager getInstance() {
		if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
			        instance = new DBConnectionManager();
                }
            }
		}
//		clientLinks++;
		return instance;
	}

	/**
	 * Initialize database properties file.
	 */
	private void init() {
		InputStream is = getClass().getResourceAsStream("/database.properties");
		Properties dbProps = new Properties();
		try {
			dbProps.load(is);
		} catch (Exception e) {
			LOGGER.error(LOG_MSG + "could not read the database.properties file. Make sure database.properties in CLASSPATH directory.");
			return;
		}
        loadDrivers(dbProps);
        createPools(dbProps);
	}

	/**
	 * load and register all drivers of JDBC.
	 * @param props properties
	 */
	private void loadDrivers(Properties props) {
		String driverClasses = props.getProperty("driver");
		StringTokenizer st = new StringTokenizer(driverClasses);
		while (st.hasMoreElements()) {
			String driverClassName = st.nextToken().trim();
			try {
				Driver driver = (Driver) Class.forName(driverClassName)
						.newInstance();
				DriverManager.registerDriver(driver);
				drivers.addElement(driver);
				LOGGER.info(LOG_MSG + "Register JDBC Driver success: " + driverClassName);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
				LOGGER.error(LOG_MSG + "Register JDBC Driver failed: " + driverClassName);
			}
		}
	}

	/**
	 * Create pool with properties.
	 */
	private void createPools(Properties props) {		
		Enumeration<?> propNames = props.propertyNames();
		while (propNames.hasMoreElements()) {
			String name = (String) propNames.nextElement();
			if (name.endsWith(".url")) {
				String poolName = name.substring(0, name.lastIndexOf("."));
				String url = props.getProperty(poolName + ".url");
				if (url == null) {
					LOGGER.info(LOG_MSG + "No url for the pool : " + poolName);
					continue;
				}
				String user = props.getProperty(poolName + ".user");
				String password = props.getProperty(poolName + ".password");
				String maxconn = props.getProperty(poolName + ".maxconn", "10");
				int max;
				try {
                    max = Integer.valueOf(maxconn);
				} catch (NumberFormatException e) {
					LOGGER.error(LOG_MSG + "error max connection limit: " + maxconn + " . pool: " + poolName);
					max = 0;
				}
				DBConnectionPool pool = new DBConnectionPool(poolName, url,
						user, password, max);
				pools.put(poolName, pool);
				LOGGER.info(LOG_MSG + "create pool success :" + poolName);
			}
		}
	}

	/**
	 * Free connnection and return to the pool.
	 */
	public void freeConnection(String pooName, Connection con) {
		DBConnectionPool pool = pools.get(pooName);
		if (pool != null) {
			pool.freeConnection(con);
		} else {
            LOGGER.info(LOG_MSG + "Count not find the pool : " + pooName);
		}
	}

	/**
	 * Get a useful connection. If there is no useful connection, and connections' number bigger than max limit, create new connection and return it. Otherwise wait another connection from other thread in default wait time.
	 * @return An useful connection or null
	 */
	public Connection getConnection(String pooName) {
		DBConnectionPool pool = pools.get(pooName);
		if (pool != null) {
			return pool.getConnection(WAIT_TIME * 1000);
		} else {
			LOGGER.info(LOG_MSG + "Could not find the pool : " + pooName);
			return null;
		}
	}

	/**
	 * 获得一个可用连接.若没有可用连接,且已有连接数小于最大连接数限制, 则创建并返回新连接.否则,在指定的时间内等待其它线程释放连接.
	 * 
	 * @param poolName
	 *            连接池名字
	 * @param time
	 *            以毫秒计的等待时间
	 * @return Connection 可用连接或null
	 */
	public Connection getConnection(String poolName, long time) {
		DBConnectionPool pool = pools.get(poolName);
		if (pool != null) {
			return pool.getConnection(time);
		}
		return null;
	}

	/**
	 * 关闭所有连接,撤销驱动程序的注册
	 */
	public synchronized void release() {
		// 等待直到最后一个客户程序调用
//		if (--clientLinks != 0) {
//			return;
//		}

		Enumeration<DBConnectionPool> allPools = pools.elements();
		while (allPools.hasMoreElements()) {
			DBConnectionPool pool = allPools.nextElement();
			pool.release();
		}
		Enumeration<Driver> allDrivers = drivers.elements();
		while (allDrivers.hasMoreElements()) {
			Driver driver = allDrivers.nextElement();
			try {
				DriverManager.deregisterDriver(driver);
				LOGGER.info("撤销JDBC驱动程序 " + driver.getClass().getName() + "的注册");
			} catch (SQLException e) {
				LOGGER.info("无法撤销下列JDBC驱动程序的注册: " + driver.getClass().getName() + "\n" + e.getStackTrace());
			}
		}
	}

	/**
	 * 此内部类定义了一个连接池.它能够根据要求创建新连接,直到预定的最大限制值
	 */
	class DBConnectionPool {
		private int checkedOut;
		private Vector<Connection> freeConnections = new Vector<Connection>();
		private int maxConn;
		private String poolName;
		private String password;
		private String URL;
		private String user;

		/**
		 * 创建新的连接池
		 * 
		 * @param poolName
		 *            连接池名字
		 * @param URL
		 *            数据库的JDBC URL
		 * @param user
		 *            数据库帐号,或 null
		 * @param password
		 *            密码,或 null
		 * @param maxConn
		 *            此连接池允许建立的最大连接数
		 */
		public DBConnectionPool(String poolName, String URL, String user,
				String password, int maxConn) {
			this.poolName = poolName;
			this.URL = URL;
			this.user = user;
			this.password = password;
			this.maxConn = maxConn;
		}

		/**
		 * 将不再使用的连接返回给连接池
		 * 
		 * @param con 客户程序释放的连接
		 */
		public synchronized void freeConnection(Connection con) {
			// 将指定连接加入到向量末尾
			freeConnections.addElement(con);
			checkedOut--;
			notifyAll();
		}

		/**
		 * 从连接池获得一个可用连接.如没有空闲的连接且当前连接数小于最大连接 数限制,则创建新连接.
		 * 如原来登记为可用的连接不再有效,则从向量删除之, 然后递归调用自己以尝试新的可用连接.
		 */
		public synchronized Connection getConnection() {
			Connection con = null;
			if (freeConnections.size() > 0) {// 获取向量中第一个可用连接
				con = freeConnections.firstElement();
				freeConnections.removeElementAt(0);
				try {
					if (con.isClosed()) {
						LOGGER.info("从连接池" + poolName + "删除一个无效连接");
						// 递归调用自己,尝试再次获取可用连接
						con = getConnection();
					}
				} catch (SQLException e) {
					LOGGER.info("从连接池" + poolName + "删除一个无效连接");
					// 递归调用自己,尝试再次获取可用连接
					con = getConnection();
				}
			} else if (maxConn == 0 || checkedOut < maxConn) {
				con = newConnection();
			}
			if (con != null) {
				checkedOut++;
			}
			return con;
		}

		/**
		 * 从连接池获取可用连接.可以指定客户程序能够等待的最长时间 参见前一个getConnection()方法.
		 * 
		 * @param timeout
		 *            以毫秒计的等待时间限制
		 */
		public synchronized Connection getConnection(long timeout) {
			long startTime = new Date().getTime();
			Connection con;
			while ((con = getConnection()) == null) {
				try {
					wait(timeout);
				} catch (InterruptedException e) {
                    LOGGER.info(e.getMessage());
				}
				if ((new Date().getTime() - startTime) >= timeout) {// wait()返回的原因是超时
                    LOGGER.info("获取连接超时");
					return null;
				}
			}
			return con;
		}

		/**
		 * 关闭所有连接
		 */
		public synchronized void release() {
			Enumeration<Connection> allConnections = freeConnections.elements();
			while (allConnections.hasMoreElements()) {
				Connection con = allConnections.nextElement();
				try {
					con.close();
                    LOGGER.info("关闭连接池" + poolName + "中的一个连接");
				} catch (SQLException e) {
                    LOGGER.info("无法关闭连接池" + poolName + "中的连接" + "\n");
                    LOGGER.info(e.getMessage());
				}
			}
			freeConnections.removeAllElements();
		}

		/**
		 * 创建新的连接
		 */
		private Connection newConnection() {
			Connection con;
			try {
				if (user == null || "".equals(user)) {
					con = DriverManager.getConnection(URL);
				} else {
					con = DriverManager.getConnection(URL, user, password);
				}
				count++;
				LOGGER.info("create a new connection(" + count + ") from pool : " + poolName);
			} catch (SQLException e) {
                LOGGER.info("无法创建下列URL的连接: " + URL);
                LOGGER.info(e.getMessage());
				return null;
			}
			return con;
		}
	}
}