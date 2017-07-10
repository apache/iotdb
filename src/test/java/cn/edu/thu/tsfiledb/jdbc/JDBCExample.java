package cn.edu.thu.tsfiledb.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCExample {
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCExample.class);
	private static String schemaFilePath = "src/main/resources/schema.csv";
	private static String sourceFilePath = "D:\\yingyeda";
	private static String host = "127.0.0.1";
	private static String port = "6667";
	private static String username = "root";
	private static String password = "root";

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
		createSchema(schemaFilePath, connection);
		insertData(sourceFilePath);
		mergeData(connection);
		connection.close();
	}

	private static void mergeData(Connection connection) throws SQLException {

		String mergeSql = "merge";
		Statement statement = connection.createStatement();
		statement.execute(mergeSql);
		statement.close();
	}

	private static void insertData(String sourcePath) throws IOException {
		File file = new File(sourcePath);
		if (!file.isDirectory()) {
			throw new RuntimeException("Source File is not directory");
		}
		List<Thread> list = new ArrayList<>();
		for (File subFile : file.listFiles()) {
			Connection connection = null;
			try {
				connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username,
						password);
			} catch (SQLException e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage());
				exception(e.getMessage());
			}
			InsertThread insertThread = new InsertThread(subFile.getAbsolutePath(), connection);
			Thread thread = new Thread(insertThread, subFile.getCanonicalPath());
			list.add(thread);
			thread.start();
		}
		for (Thread thread : list) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage());
				exception(e.getMessage());
			}
		}
	}

	private static void exception(String message) {
		throw new RuntimeException(message);
	}

	private static String[] timeSeries = { "status WITH DATATYPE=BYTE_ARRAY, ENCODING=PLAIN",
			"errtype WITH DATATYPE=INT32, ENCODING=RLE", "v WITH DATATYPE=INT64, ENCODING=RLE",
			"a WITH DATATYPE=FLOAT, ENCODING=RLE", "h WITH DATATYPE=FLOAT, ENCODING=RLE",
			"px WITH DATATYPE=FLOAT, ENCODING=RLE", "py WITH DATATYPE=FLOAT, ENCODING=RLE",
			"H_Result WITH DATATYPE=INT32, ENCODING=RLE", "A_Result WITH DATATYPE=INT32, ENCODING=RLE",
			"V_Result WITH DATATYPE=INT32, ENCODING=RLE", "X_Result WITH DATATYPE=INT32, ENCODING=RLE",
			"Y_Result WITH DATATYPE=INT32, ENCODING=RLE", "Bridge_Result WITH DATATYPE=INT32, ENCODING=RLE",
			"Pad_W WITH DATATYPE=FLOAT, ENCODING=RLE", "Pad_H WITH DATATYPE=FLOAT, ENCODING=RLE" };

	private static void createSchema(String filePath, Connection connection) throws IOException {
		File file = new File(filePath);
		BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		String line = bufferedReader.readLine();
		Statement statement = null;
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
		while ((line = bufferedReader.readLine()) != null) {
			String[] words = line.split(",");
			String deviceid = "CREATE TIMESERIES root.Inventec." + words[0] + "_" + words[1] + ".";
			for (String s : timeSeries) {
				String sql = deviceid + s;
				try {
					statement.executeUpdate(sql);
				} catch (SQLException e) {
					e.printStackTrace();
					LOGGER.error(e.getMessage());
				}
			}
		}
		bufferedReader.close();
		String fileLevel = "SET STORAGE GROUP TO root.Inventec";
		try {
			statement.executeUpdate(fileLevel);
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
		try {
			statement.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
	}

	public static class InsertThread implements Runnable {

		private String sourcePath;
		private Connection connection;
		private Statement statement;
		private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		public InsertThread(String sourcePath, Connection connection) {
			this.sourcePath = sourcePath;
			this.connection = connection;
			try {
				statement = connection.createStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage());
			}
		}

		@Override
		public void run() {

			File file = new File(sourcePath);
			BufferedReader bufferedReader = null;
			try {
				bufferedReader = new BufferedReader(new FileReader(file));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage());
			}
			String line = null;
			try {
				line = bufferedReader.readLine();
				System.out.println(line);
				while ((line = bufferedReader.readLine()) != null) {
					System.out.println(line);
					insertData(line);
				}
				statement.close();
				connection.close();
			} catch (IOException | SQLException e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage());
				exception(e.getMessage());
			}
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
					LOGGER.error(e.getMessage());
				}
			}
		}

		private String timeTrans(String time) {

			try {
				return String.valueOf(dateFormat.parse(time).getTime());
			} catch (ParseException e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage());
				exception(e.getMessage());
			}
			return time;
		}

		private void insertData(String line) {
			String[] words = line.split(",");
			for (int i = 0; i < words.length; i++) {
				String word = words[i];
				if (word.startsWith(".")) {
					words[i] = "0" + word;
				}
				if (word.startsWith("-.")) {
					words[i] = "-0" + word.substring(1, word.length());
				}
			}
			words[0] = timeTrans(words[0]);
			String insertSql = String.format(
					"insert into root.Inventec.%s_%s"
							+ "(timestamp,status,errtype,v,a,h,px,py,H_Result,A_Result,V_Result,X_Result,Y_Result,Bridge_Result,Pad_W,Pad_H) "
							+ "VALUES (%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
					words[1], words[2], words[0], words[3], words[4], words[5], words[6], words[7], words[8], words[9],
					words[10], words[11], words[12], words[13], words[14], words[15], words[16], words[17]);
			try {
				
				statement.executeUpdate(insertSql);
			} catch (SQLException e) {
				System.out.println(e);
				LOGGER.error(insertSql);
				LOGGER.error(e.getMessage());
			}
		}

	}
}
