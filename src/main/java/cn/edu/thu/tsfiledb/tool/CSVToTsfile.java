package cn.edu.thu.tsfiledb.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * CSV File To TsFileDB
 * 
 * @author zhanggr
 *
 */
public class CSVToTsfile {

	private static final Logger LOGGER = LoggerFactory.getLogger(CSVToTsfile.class);

	private static final String HOST_ARGS = "h";
	private static final String HOST_NAME = "host";

	private static final String HELP_ARGS = "help";

	private static final String PORT_ARGS = "p";
	private static final String PORT_NAME = "port";

	private static final String PASSWORD_ARGS = "pw";
	private static final String PASSWORD_NAME = "password";

	private static final String USERNAME_ARGS = "u";
	private static final String USERNAME_NAME = "username";

	private static final String FILE_ARGS = "f";
	private static final String FILE_NAME = "file";

	private static final String TIMEFORMAT_ARGS = "t";
	private static final String TIMEFORMAT_NAME = "timeformat";

	private static final int MAX_HELP_CONSOLE_WIDTH = 88;
	private static final String TSFILEDB_CLI_PREFIX = "CSV_To_TsFile";

	private static final String STRING_DATA_TYPE = "TEXT";
	private static final int BATCH_EXCUTE_COUNT = 10;

	private static String host;
	private static String port;
	private static String username;
	private static String password;
	private static String filename;
	private static String timeformat = "timestamps";
	private static String errorInsertInfo = "${TSFILE_HOME}/csvInsertError.txt";
	
	private static HashMap<String, String> timeseriesToType = null;
	
	private static HashMap<String, ArrayList<Integer>> deviceToColumn;  // storage csv table head info
	private static ArrayList<String> headInfo; // storage csv table head info
	private static ArrayList<String> colInfo; // storage csv device sensor info, corresponding csv table head

	/**
	 * commandline option create
	 * 
	 * @return object Options
	 */
	private static Options createOptions() {
		Options options = new Options();
		Option help = new Option(HELP_ARGS, false, "Display help information");
		help.setRequired(false);
		options.addOption(help);

		 Option opHost = Option.builder(HOST_ARGS).argName(HOST_NAME).hasArg().desc("Host Name (required)").build();
		options.addOption(opHost);

		Option opPort = Option.builder(PORT_ARGS).argName(PORT_NAME).hasArg().desc("Port (required)").build();
		options.addOption(opPort);

		Option opUsername = Option.builder(USERNAME_ARGS).argName(USERNAME_NAME).hasArg().desc("User Name (required)").build();
		options.addOption(opUsername);

		Option opPassword = Option.builder(PASSWORD_ARGS).optionalArg(true).argName(PASSWORD_NAME).hasArg().desc("Password (optional)").build();
		options.addOption(opPassword);

		Option opFile = Option.builder(FILE_ARGS).optionalArg(true).argName(FILE_NAME).hasArg().desc("csv file path (required)").build();
		options.addOption(opFile);

		Option opTimeformat = Option.builder(TIMEFORMAT_ARGS).optionalArg(true).argName(TIMEFORMAT_NAME).hasArg().desc("timeFormat  (not required)").build();
		options.addOption(opTimeformat);

		return options;
	}

	/**
	 * 
	 * @param tf time format, optioned:ISO8601, timestamps, self-defined:
	 *            yyyy-mm-dd hh:mm:ss
	 * @param timestamp the time column of the csv file
	 * @return the timestamps will insert into SQL
	 */
	private static String setTimestamp(String tf, String timestamp) {
		if (tf.equals("ISO8601")) {
			return timestamp;
		} else if (tf.equals("timestamps")) {
			try {
				Long.parseLong(timestamp);				
			}catch(NumberFormatException nfe) {
				LOGGER.error("time format exception, don't insert " + timestamp);
				return "";
			}
			return timestamp;
		} else {
			SimpleDateFormat sdf = new SimpleDateFormat(tf);
			Date date = null;
			try {
				date = sdf.parse(timestamp);
			} catch (java.text.ParseException e) {
				LOGGER.error("input time format error please re-input, Unparseable date: " + tf);
			}
			if (date != null)
				return date.getTime() + "";
			return "";
		}
	}

	/**
	 * Data from csv To tsfile
	 */
	public static void loadDataFromCSV() {
		Connection connection = null;
		Statement statement = null;
		BufferedReader br = null;
		BufferedWriter bw = null;
		File file = new File(errorInsertInfo);
		try {
			Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
			connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
			br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filename))));
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
			String line = "";
			String[] strHeadInfo = br.readLine().split(",");
			deviceToColumn = new HashMap<String, ArrayList<Integer>>();
			colInfo = new ArrayList<String>();
			headInfo = new ArrayList<String>();

			if (strHeadInfo.length <= 1) {
				LOGGER.error("The CSV file illegal");
				br.close();
				connection.close();
				System.exit(1);
			}

			timeseriesToType = new HashMap<String, String>();
			DatabaseMetaData databaseMetaData = connection.getMetaData();

			for (int i = 1; i < strHeadInfo.length; i++) {

				ResultSet resultSet = databaseMetaData.getColumns(null, null, strHeadInfo[i], null);
				if (resultSet.next()) {
					timeseriesToType.put(resultSet.getString(0), resultSet.getString(1));
				} else {
					LOGGER.error("database Cannot find " + strHeadInfo[i] + ",stop import!");
					bw.write("database Cannot find " + strHeadInfo[i] + ",stop import!");
					br.close();
					bw.close();
					connection.close();
					System.exit(1);
				}
				headInfo.add(strHeadInfo[i]);
				String deviceInfo = strHeadInfo[i].substring(0, strHeadInfo[i].lastIndexOf("."));
				
				if (!deviceToColumn.containsKey(deviceInfo)) {
					deviceToColumn.put(deviceInfo, new ArrayList<Integer>());
				}
				// storage every device's sensor index info
				deviceToColumn.get(deviceInfo).add(i - 1);
				colInfo.add(strHeadInfo[i].substring(strHeadInfo[i].lastIndexOf(".") + 1));
			}

			statement = connection.createStatement();
			int count =0;
			while ((line = br.readLine()) != null) {
				ArrayList<String> sqls = createInsertSQL(line,bw);
				for (String str : sqls) {
					try {
						count++;
						statement.addBatch(str);
						if(count == BATCH_EXCUTE_COUNT) {
							statement.executeBatch();
							statement.clearBatch();
							count = 0;
						}
					} catch (SQLException e) {
						LOGGER.error("{} :excuted fail!");
						bw.write(e.getMessage());
						bw.newLine();
					}
				}
			}
			try {
				statement.executeBatch();
				LOGGER.debug("excuted finish!");
			} catch (SQLException e) {
				bw.write(e.getMessage());
				bw.newLine();
			}
			
		} catch (FileNotFoundException e) {
			LOGGER.error("The csv file {} can't find!",filename, e);
		} catch (IOException e) {
			LOGGER.error("CSV file read exception!", e);
		} catch (ClassNotFoundException e2) {
			LOGGER.error("Cannot find cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		} catch (SQLException e) {
			LOGGER.error("database connection exception!", e);
		} finally {
			try {
				bw.close();
				statement.close();
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if(file.exists() && (file.length() == 0)) {
			file.delete();
		}

	}

	private static String typeIdentify(String timeseries) {
		return timeseriesToType.get(timeseries);

	}

	/**
	 * create Insert SQL statement according to every line csv data
	 * 
	 * @param line csv line data
	 * @param bwToErrorFile the error info insert to file
	 * @return ArrayList<String> SQL statement list
	 * @throws IOException 
	 */
	private static ArrayList<String> createInsertSQL(String line, BufferedWriter bwToErrorFile) throws IOException {
		String[] data = line.split(",", headInfo.size() + 1);

		ArrayList<String> sqls = new ArrayList<String>();
		Iterator<Map.Entry<String, ArrayList<Integer>>> it = deviceToColumn.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, ArrayList<Integer>> entry = it.next();
			StringBuilder sbd = new StringBuilder();
			ArrayList<Integer> colIndex = entry.getValue();
			sbd.append("insert into " + entry.getKey() + "(timestamp");
			int skipcount = 0;
			for (int j = 0; j < colIndex.size(); ++j) {
				if (data[entry.getValue().get(j) + 1].equals("")) {
					skipcount++;
					continue;
				}
				sbd.append(", " + colInfo.get(colIndex.get(j)));
			}
			// define every device null value' number, if the number equal the
			// sensor number, the insert operation stop
			if (skipcount == entry.getValue().size())
				continue;

			String timestampsStr = setTimestamp(timeformat, data[0]);
			if (timestampsStr == "") {
				LOGGER.error("Time Format Error!");
				bwToErrorFile.write("Time Format Error!");
				bwToErrorFile.newLine();
				continue;
			}
			sbd.append(") values(").append(timestampsStr);

			for (int j = 0; j < colIndex.size(); ++j) {
				if (data[entry.getValue().get(j) + 1].equals(""))
					continue;
				if (typeIdentify(headInfo.get(colIndex.get(j))) == STRING_DATA_TYPE) {
					sbd.append(", \'" + data[colIndex.get(j) + 1] + "\'");
				} else {
					sbd.append("," + data[colIndex.get(j) + 1]);
				}
			}
			sbd.append(")");
			sqls.add(sbd.toString());
		}

		return sqls;
	}

	public static void main(String[] args) {

		Options options = createOptions();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
		CommandLine commandLine = null;
		CommandLineParser parser = new DefaultParser();
		Scanner scanner = new Scanner(System.in);

		if (args == null || args.length == 0) {
			hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
			scanner.close();
			return;
		}
		
		try {
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
			LOGGER.error("problems encountered while parsing the command line tokens.", e);
			scanner.close();
			System.exit(1);
		}
		if (commandLine.hasOption(HELP_ARGS)) {
			hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
			scanner.close();
			return;
		}
		 host = commandLine.getOptionValue(HOST_ARGS);
		 port = commandLine.getOptionValue(PORT_ARGS);
		 username = commandLine.getOptionValue(USERNAME_ARGS);
		 password = commandLine.getOptionValue(PASSWORD_ARGS);
		 if (password == null) {
			 System.out.print(TSFILEDB_CLI_PREFIX + "> please input password: ");
			 password = scanner.nextLine();
		 }
		 filename = commandLine.getOptionValue(FILE_ARGS);
		 if(host == null || port == null || username == null || filename == null) {
			 hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
			 scanner.close();
			 return;
		 }
		timeformat = commandLine.getOptionValue(TIMEFORMAT_ARGS);
		if(timeformat == null) {
			timeformat = "timestamps";
		}
		 
		loadDataFromCSV();
	}
}
