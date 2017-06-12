package cn.edu.thu.tsfiledb.auth.dao;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

/**
 * @author liukun
 *
 */
public class DBdao {

	private String derbyEmbeddedDriver = "org.apache.derby.jdbc.EmbeddedDriver";
	private String protocal = "jdbc:derby:";
	private String DBName;
	private String createOrNot = ";create=true";
	private String shutdown = ";shutdown=True";

	private static Connection connection = null;
	private static Statement statement = null;
	private PreparedStatement preparedStatement = null;

	/**
	 * @param dBName
	 */
	public DBdao(String dBName) {
		String derbyDirPath = TsfileDBDescriptor.getInstance().getConfig().derbyHome;
		if (derbyDirPath.length() > 0 && derbyDirPath.charAt(derbyDirPath.length() - 1) != File.separatorChar) {
			derbyDirPath = derbyDirPath + File.separatorChar;
		}
		String path = derbyDirPath + dBName;
		DBName = path;
	}

	public DBdao() {
		this("derby-tsfile-db");
	}

	private void initDriver() {
		try {
			Class.forName(derbyEmbeddedDriver).newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void connection() {
		String url = protocal + DBName + createOrNot;
		try {
			connection = DriverManager.getConnection(url);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void closeConnection() {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else {
			try {
				throw new Exception("The connection is null");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void statement() {
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void closeStatement() {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else {
			try {
				throw new Exception("The statement is null");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private boolean checkTableExist() {
		boolean state = false;

		try {
			DatabaseMetaData metaData = connection.getMetaData();
			ResultSet resultSet;
			resultSet = metaData.getTables(null, "APP", "USERTABLE", null);
			if (resultSet.next()) {
				state = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return state;
	}

	// create table;
	public boolean createOriTable() {
		boolean state = false;
		try {
			statement.executeUpdate(InitTable.createUserTableSql);
			statement.executeUpdate(InitTable.createRoleTableSql);
			statement.executeUpdate(InitTable.createUserRoleRelTableSql);
			statement.executeUpdate(InitTable.creteUserPermissionTableSql);
			statement.executeUpdate(InitTable.createRolePermissionTableSql);
			statement.executeUpdate(InitTable.insertIntoUserToTableSql);
			state = true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return state;
	}

	public void getPreparedStatement() {
		try {
			preparedStatement = connection.prepareStatement("test");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void closePreparedStatement() {

	}

	public void open() {
		initDriver();
		connection();
		statement();
		if (!checkTableExist()) {
			//
			createOriTable();
		}
	}

	public void close() {
		closeStatement();
		closeConnection();
		// try {
		// DriverManager.getConnection(protocal + shutdown);
		// } catch (SQLException e) {
		// e.printStackTrace();
		// }
	}

	public static Statement getStatement() {
		return statement;
	}

	public static Connection getConnection() {
		return connection;
	}

}
