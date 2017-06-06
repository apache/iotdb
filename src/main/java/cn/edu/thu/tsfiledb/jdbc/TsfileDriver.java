package cn.edu.thu.tsfiledb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.thrift.transport.TTransportException;


public class TsfileDriver implements Driver {
	private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TsfileDriver.class);
	
	static {
		try {
			java.sql.DriverManager.registerDriver(new TsfileDriver());
		} catch (SQLException e) {
			LOGGER.error("Error occurs when resgistering TsFile driver",e);
		}
	}
	
	/**
	 * Is this driver JDBC compliant?
	 */
	private static final boolean TSFILE_JDBC_COMPLIANT = false;
	
	public TsfileDriver() {

	}
	
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return Pattern.matches(TsfileConfig.TSFILE_URL_PREFIX+".*", url);
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		try {
			return acceptsURL(url) ? new TsfileConnection(url, info) : null;
		} catch (TTransportException e) {
			throw new SQLException("Connection Error, Please check whether the network is avalible or the server has started. " + e.getMessage());
		}
	}

	@Override
	public int getMajorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMinorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException("Method not supported");
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean jdbcCompliant() {
		return TSFILE_JDBC_COMPLIANT;
	}

}
