package cn.edu.thu.tsfiledb.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Demo {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
//		Statement statement = connection.createStatement();
//		statement.addBatch("123");
//		statement.addBatch("321");
//		int[] resultSet = statement.executeBatch();
//		System.out.println(resultSet.length);
//		statement.clearBatch();
//		connection.close();
//		
//		Connection connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.15:6667/","root","root");
		DatabaseMetaData databaseMetaData = connection.getMetaData();
		System.out.println("show metadata");
		System.out.println(databaseMetaData);
		// get all columns
		ResultSet resultSet = databaseMetaData.getColumns(null, null, "car", null);
		while(resultSet.next()){
			System.out.println(String.format("column %s, type %s", resultSet.getString("COLUMN_NAME"), resultSet.getString("COLUMN_TYPE")));
		}
		// get all delta object
		resultSet = databaseMetaData.getColumns(null, null, null, "car");
		while(resultSet.next()){
			System.out.println(String.format("delta object %s", resultSet.getString("DELTA_OBJECT")));
		}
		connection.close();

	}

}
