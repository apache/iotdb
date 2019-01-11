package cn.edu.tsinghua.iotdb.jdbc.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class StatementDemo {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
		Connection connection = null;
		try {
			connection =  DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
			Statement statement = connection.createStatement();
			statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt01");
			statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
			statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
			statement.execute("insert into root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)");
			statement.execute("insert into root.ln.wf01.wt01(timestamp,status) values(1509465660000,true)");
			statement.execute("insert into root.ln.wf01.wt01(timestamp,status) values(1509465720000,false)");
			statement.execute("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600000,25.957603)");
			statement.execute("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465660000,24.359503)");
			statement.execute("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465720000,20.092794)");
			ResultSet resultSet = statement.executeQuery("select * from root");
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while(resultSet.next()){
				StringBuilder  builder = new StringBuilder();
				for (int i = 1; i <= resultSetMetaData.getColumnCount();i++) {
					builder.append(resultSet.getString(i)).append(",");
				}
				System.out.println(builder);
			}
			statement.close();
			
		} finally {
			connection.close();
		}
	}
}
