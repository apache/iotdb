package cn.edu.thu.tsfiledb.jdbc;

public class TsfileJDBCConfig {
	/**
	 * The required prefix for the connection URL.
	 */
	public static final String TSFILE_URL_PREFIX = "jdbc:tsfile://";

	public static final String TSFILE_DEFAULT_HOST = "localhost";
	/**
	 * If host is provided, without a port.
	 */
	public static final int TSFILE_DEFAULT_PORT = 6667;

	/**
	 * tsfile's default series name
	 */
	public static final String DEFAULT_SERIES_NAME = "default";
		
	public static final String AUTH_USER = "user";
	public static final String DEFAULT_USER = "user";
	
	public static final String AUTH_PASSWORD = "password";
	public static final String DEFALUT_PASSWORD = "password";
	
	public static final int RETRY_NUM = 3;
	public static final long RETRY_INTERVAL = 1000;
	
	public static int fetchSize = 10000;
	public static int connectionTimeoutInMs = 0;
}
