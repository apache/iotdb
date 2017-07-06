package cn.edu.thu.tsfiledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCExample {
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCExample.class);
	private static int count = 0;

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		String host = "127.0.0.1";
		String port = "6667";
		String username = "root";
		String password = "root";
		connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
		createSchema(connection);
		insertData(connection);
		LOGGER.info("write row count is {}", count);
		mergeData(connection);
		connection.close();
	}

	public static void mergeData(Connection connection) throws SQLException {

		String mergeSql = "merge";
		Statement statement = connection.createStatement();
		statement.execute(mergeSql);
		statement.close();
	}

	public static void insertData(Connection connection) throws IOException {
		Statement statement = null;
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		String csvFilePath = "src/main/resources/out.csv";
		File csvFile = new File(csvFilePath);
		FileReader fileReader = new FileReader(csvFile);
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(fileReader);
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] words = line.split(",");
			for(int i = 0;i<words.length;i++){
				String word = words[i];
				if(word.startsWith(".")){
					words[i] = "0"+word;
				}
				if(word.startsWith("-.")){
					words[i] = "-0"+word.substring(1, word.length());
				}
			}
			String insertSql = String.format(
					"insert into root.Inventec_boot.U8113_1(timestamp,v,a,h,px,py,status) VALUES (%s,%s,%s,%s,%s,%s,%s)",
					words[0].toString(), words[3].toString(), words[4].toString(), words[5].toString(),
					words[6].toString(), words[7].toString(), words[8].toString());
			try {
				statement.executeUpdate(insertSql);
				count++;
			} catch (SQLException e) {
				LOGGER.error(insertSql);
			}
		}
		reader.close();
		try {
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void createSchema(Connection connection) {
		Statement statement = null;
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		String[] timeseries = { "CREATE TIMESERIES root.Inventec_boot.U8113_1.v WITH DATATYPE=FLOAT, ENCODING=RLE",
				"CREATE TIMESERIES root.Inventec_boot.U8113_1.a WITH DATATYPE=FLOAT, ENCODING=RLE",
				"CREATE TIMESERIES root.Inventec_boot.U8113_1.h WITH DATATYPE=FLOAT, ENCODING=RLE",
				"CREATE TIMESERIES root.Inventec_boot.U8113_1.px WITH DATATYPE=FLOAT, ENCODING=RLE",
				"CREATE TIMESERIES root.Inventec_boot.U8113_1.py WITH DATATYPE=FLOAT, ENCODING=RLE",
				"CREATE TIMESERIES root.Inventec_boot.U8113_1.status WITH DATATYPE=INT32, ENCODING=RLE" };
		for (String sql : timeseries) {
			try {
				statement.executeUpdate(sql);
			} catch (SQLException e) {

			}
		}
		// set file level
		String fileLevel = "SET STORAGE GROUP TO root.Inventec_boot.U8113_1";
		try {
			statement.executeUpdate(fileLevel);
		} catch (SQLException e) {
		}
		try {
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
