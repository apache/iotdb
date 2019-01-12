package org.apache.iotdb.jdbc;

public class IoTDBConnectionParams {
    private String host = Config.IOTDB_URL_PREFIX;
    private int port = Config.IOTDB_DEFAULT_PORT;
    private String jdbcUriString;
    private String seriesName = Config.DEFAULT_SERIES_NAME;
    private String username = Config.DEFAULT_USER;
    private String password = Config.DEFALUT_PASSWORD;
    
    public IoTDBConnectionParams(String url){
    	this.jdbcUriString = url;
    }
    
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getJdbcUriString() {
		return jdbcUriString;
	}
	public void setJdbcUriString(String jdbcUriString) {
		this.jdbcUriString = jdbcUriString;
	}
	public String getSeriesName() {
		return seriesName;
	}
	public void setSeriesName(String seriesName) {
		this.seriesName = seriesName;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}


}
