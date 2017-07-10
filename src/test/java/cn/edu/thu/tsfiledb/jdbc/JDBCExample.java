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
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCExample {
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCExample.class);
	private static String schemaFilePath = "src/main/resources/schema.csv";
	private static String sourceFilePath = "D:\\datayingyeda";
	private static String host = "127.0.0.1";
	private static String port = "6667";
	private static String username = "root";
	private static String password = "root";
	private static Set<String> set = new HashSet<>();
	private static long count = 0;
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
		createSchema(schemaFilePath, connection);
		connection.close();
		insertData(sourceFilePath);
		// mergeData(connection);

	}

	private static void mergeData(Connection connection) throws SQLException {

		String mergeSql = "merge";
		Statement statement = connection.createStatement();
		statement.execute(mergeSql);
		statement.close();
	}

	private static String timeTrans(String time) {

		try {
			return String.valueOf(dateFormat.parse(time).getTime());
		} catch (ParseException e) {
			LOGGER.error(e.getMessage());
		}
		return time;
	}

	private static void insertOneData(String line, Statement statement) {
		if (line.indexOf('E') != -1) {
			return;
		}
		String[] words = line.split(",");
		if (words.length != 18) {
			return;
		}
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
			set.add(words[1] + "_" + words[2]);
			count++;
			if (count % 10000 == 0) {
				System.out.println("The size of set is " + set.size() + ", The count is " + count);
			}
		} catch (SQLException e) {
			LOGGER.error(insertSql);
			LOGGER.error(e.getMessage());
		}
	}

	private static void insertData(String sourcePath) throws IOException {
		File file = new File(sourcePath);
		if (!file.isDirectory()) {
			throw new RuntimeException("Source File is not directory");
		}
		int filecount = 0;
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
			Statement statement = null;
			try {
				statement = connection.createStatement();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			BufferedReader bufferedReader = null;
			filecount++;
			System.out.println("File name is " + subFile.getCanonicalPath() + " count is " + filecount);
			try {
				bufferedReader = new BufferedReader(new FileReader(subFile));
				String line = bufferedReader.readLine();
				while ((line = bufferedReader.readLine()) != null) {
					insertOneData(line, statement);
				}
				statement.close();
			} catch (FileNotFoundException e) {
				LOGGER.error(e.getMessage());
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static void exception(String message) {
		throw new RuntimeException(message);
	}

	private static String[] timeSeries = { "status WITH DATATYPE=BYTE_ARRAY, ENCODING=PLAIN",
			"errtype WITH DATATYPE=INT32, ENCODING=RLE", "v WITH DATATYPE=FLOAT, ENCODING=RLE",
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
}
