package cn.edu.thu.tsfiledb.jdbc;

public class TsfileConnectionParams {
    private String host = TsfileJDBCConfig.TSFILE_DEFAULT_HOST;
    private int port = TsfileJDBCConfig.TSFILE_DEFAULT_PORT;
    private String jdbcUriString;
    private String seriesName = TsfileJDBCConfig.DEFAULT_SERIES_NAME;
    private String username = TsfileJDBCConfig.DEFAULT_USER;
    private String password = TsfileJDBCConfig.DEFALUT_PASSWORD;
    
    public TsfileConnectionParams(String url){
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
