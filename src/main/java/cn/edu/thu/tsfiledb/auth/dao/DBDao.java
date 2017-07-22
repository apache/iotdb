package cn.edu.thu.tsfiledb.auth.dao;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth.AuthException;
import cn.edu.thu.tsfiledb.auth.AuthRuntimeException;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

/**
 * @author liukun
 *
 */
public class DBDao {

	private static final Logger LOGGER = LoggerFactory.getLogger(DBDao.class);

	private String derbyEmbeddedDriver = "org.apache.derby.jdbc.EmbeddedDriver";
	private String protocal = "jdbc:derby:";
	private String DBLocalPath;
	private String createOrNot = ";create=true";
	private String shutdown = ";shutdown=True";

	private static Connection connection = null;
	private static Statement statement = null;
	private PreparedStatement preparedStatement = null;

	private DBDao(String dBName) {
		String derbyDirPath = TsfileDBDescriptor.getInstance().getConfig().derbyHome;
		if (derbyDirPath.length() > 0 && derbyDirPath.charAt(derbyDirPath.length() - 1) != File.separatorChar) {
			derbyDirPath = derbyDirPath + File.separatorChar;
		}
		String path = derbyDirPath + dBName;
		this.DBLocalPath = path;
	}

	public DBDao() {
		this("derby-tsfile-db");
	}

	private void initDriver() throws ClassNotFoundException {
		try {
			Class.forName(derbyEmbeddedDriver);
		} catch (ClassNotFoundException e) {
			LOGGER.error(e.getMessage());
			throw e;
		}
	}

	private void connection() throws SQLException, DBDaoInitException{
		String url = protocal + DBLocalPath + createOrNot;
		try {
			connection = DriverManager.getConnection(url);
		} catch (SQLException e) {
			SQLException e2 = e.getNextException();
			if(e2 != null){
				// special code defined by Derby
				if(e2.getSQLState().equals("XBM0A")){
					LOGGER.error(e2.getMessage());
					try {
						FileUtils.deleteDirectory(new File(DBLocalPath));
						System.out.println(String.format("Delete %s successfully, you may restart TsFileDB now.", DBLocalPath));
					} catch (IOException e1) {
						LOGGER.error("Fail to delete {} automatically, you may delete manually and restart TsFileDB");
					} 
					throw new DBDaoInitException(e2);
				} else{
					LOGGER.error(e.getMessage());
					throw e;
				}
			} else{
				LOGGER.error(e.getMessage());
				throw e;
			}
		}
	}

	private void closeConnection() throws SQLException, AuthException {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				LOGGER.error(e.getMessage());
				throw e;
			}
		} else {
			LOGGER.error("The connection is null");
			throw new AuthException("The connection is null");
		}
	}

	private void statement() throws SQLException {
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			throw e;
		}
	}

	private void closeStatement() throws SQLException, AuthException {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				LOGGER.error(e.getMessage());
				throw e;
			}
		} else {
			LOGGER.error("The statement is null");
			throw new AuthException("The statement is null");
		}
	}

	private boolean checkTableExist() throws SQLException {

		boolean state = true;
		try {
			DatabaseMetaData metaData = connection.getMetaData();
			ResultSet resultSet;
			resultSet = metaData.getTables(null, "APP", "USERTABLE", null);
			if (!resultSet.next()) {
				statement.executeUpdate(InitTable.createUserTableSql);
				state = false;
			}
			resultSet = metaData.getTables(null, "APP", "ROLETABLE", null);
			if (!resultSet.next()) {
				statement.executeUpdate(InitTable.createRoleTableSql);
				state = false;
			}
			resultSet = metaData.getTables(null, "APP", "USERROLERELTABLE", null);
			if (!resultSet.next()) {
				statement.executeUpdate(InitTable.createUserRoleRelTableSql);
				state = false;
			}
			resultSet = metaData.getTables(null, "APP", "USERPERMISSIONTABLE", null);
			if (!resultSet.next()) {
				statement.executeUpdate(InitTable.creteUserPermissionTableSql);
				state = false;
			}
			resultSet = metaData.getTables(null, "APP", "ROLEPERMISSIONTABLE", null);
			if (!resultSet.next()) {
				statement.executeUpdate(InitTable.createRolePermissionTableSql);
				state = false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
			throw e;
		}
		return state;
	}

	private boolean createOriTable() throws SQLException {

		try {
			statement.executeUpdate(InitTable.insertIntoUserToTableSql);
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			throw e;
		}
		return true;
	}

	public void open() throws ClassNotFoundException, SQLException, DBDaoInitException{
		initDriver();
		connection();
		statement();
		if (!checkTableExist()) {
			createOriTable();
		}
	}

	public void close() {
		
		try {
			closeStatement();
			closeConnection();
		} catch (SQLException | AuthException e) {
			throw new AuthRuntimeException(e);
		}
	}

	public static Statement getStatement() {
		return statement;
	}

	public static Connection getConnection() {
		return connection;
	}

}
