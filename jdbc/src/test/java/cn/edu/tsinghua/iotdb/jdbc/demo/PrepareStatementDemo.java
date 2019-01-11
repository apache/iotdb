package cn.edu.tsinghua.iotdb.jdbc.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

public class PrepareStatementDemo {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
		Connection connection = null;
		try {
			connection =  DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
			PreparedStatement preparedStatement = connection.prepareStatement("insert into root.ln.wf01.wt01(timestamp,status,temperature) values(?,?,?)");
			preparedStatement.setLong(1, 1509465600000L);
			preparedStatement.setBoolean(2, true);
			preparedStatement.setFloat(3, 25.957603f);
			preparedStatement.execute();
			preparedStatement.clearParameters();
			
			preparedStatement.setLong(1, 1509465660000L);
			preparedStatement.setBoolean(2, true);
			preparedStatement.setFloat(3, 24.359503f);
			preparedStatement.execute();
			preparedStatement.clearParameters();
			
			preparedStatement.setLong(1, 1509465720000L);
			preparedStatement.setBoolean(2, false);
			preparedStatement.setFloat(3, 20.092794f);
			preparedStatement.execute();
			preparedStatement.clearParameters();
			
			preparedStatement.setTimestamp(1, Timestamp.valueOf("2017-11-01 00:03:00"));
			preparedStatement.setBoolean(2, false);
			preparedStatement.setFloat(3, 20.092794f);
			preparedStatement.execute();
			preparedStatement.clearParameters();
						
			preparedStatement.close();
			
			ResultSet resultSet = preparedStatement.executeQuery("select * from root");
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while(resultSet.next()){
				StringBuilder  builder = new StringBuilder();
				for (int i = 1; i <= resultSetMetaData.getColumnCount();i++) {
					builder.append(resultSet.getString(i)).append(",");
				}
				System.out.println(builder);
			}
			preparedStatement.close();
			for(int i = 1; i <= resultSetMetaData.getColumnCount();i++) {
				System.out.println(resultSetMetaData.getColumnType(i)+"-"+resultSetMetaData.getColumnName(i));
			}
		} finally {
			connection.close();
		}
	}

}
