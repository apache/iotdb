package cn.edu.thu.tsfiledb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Test {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Connection connection;
		for(int i = 0; i < 1000;i++){
			Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
			connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
//			connection.close();
		}


	}

}
