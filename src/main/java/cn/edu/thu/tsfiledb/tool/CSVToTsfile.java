package cn.edu.thu.tsfiledb.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;

/**
 * CSV File To TsFile Class
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

	private static String host;
	private static String port;
	private static String username;
	private static String password;
	private static String filename;
	private static String timeformat = "timestamps";

	private static HashMap<String, String> timeseriesToType = null;
	private static HashMap<String, ArrayList<Integer>> deviceToColumn; // storage every device column in csv file
	private static ArrayList<String> headInfo; // storage csv table head info
	private static ArrayList<String> colInfo; // storage csv device sensor info, corresponding csv table head

	private static Connection connection = null;
	private static Statement statement = null;

	/**
	 * commandline option create
	 * @return object Options
	 */
	private static Options createOptions() {
		Options options = new Options();
		Option help = new Option(HELP_ARGS, false, "Display help information");
		help.setRequired(false);
		options.addOption(help);

		Option op_host = OptionBuilder.withArgName(HOST_NAME).hasArg().withDescription("Host Name (required)")
				.create(HOST_ARGS);
		options.addOption(op_host);

		Option os_port = OptionBuilder.withArgName(PORT_NAME).hasArg().withDescription("Port (required)")
				.create(PORT_ARGS);
		options.addOption(os_port);

		Option os_username = OptionBuilder.withArgName(USERNAME_NAME).hasArg().withDescription("User name (required)")
				.create(USERNAME_ARGS);
		options.addOption(os_username);

		Option os_password = OptionBuilder.withArgName(PASSWORD_NAME).hasArg().withDescription("Password (required)")
				.create(PASSWORD_ARGS);
		options.addOption(os_password);

		Option os_file = OptionBuilder.withArgName(FILE_NAME).hasArg().withDescription("csv file path (required)")
				.create(FILE_ARGS);
		options.addOption(os_file);

		Option os_timeformat = OptionBuilder.withArgName(TIMEFORMAT_NAME).hasArg()
				.withDescription("timeFormat  (not required)").create(TIMEFORMAT_ARGS);
		options.addOption(os_timeformat);

		return options;
	}
	
	/**
	 * 
	 * @param arg argument for commandline e:h,f,p..
	 * @param name the name of argument
	 * @param commandLine
	 * @return the value of the option e: the argument h's value is 127.0.0.1
	 * @throws ArgsErrorException
	 */
	private static String checkRequiredArg(String arg, String name, CommandLine commandLine) throws ArgsErrorException {
		String str = commandLine.getOptionValue(arg);
		if (str == null) {
			String msg = String.format("%s: Required values for option '%s' not provided", TSFILEDB_CLI_PREFIX, name);
			System.out.println(msg);
			System.out.println("Use -help for more information");
			throw new ArgsErrorException(msg);
		}
		return str;
	}

	
	/**
	 * 
	 * @param tf the time format, optioned:ISO8601, timestamps, self-defined: yyyy-mm-dd hh:mm:ss
	 * @param words the time column of the csv file
	 * @return the timestamps will insert into SQL
	 */
	private static String setTimeFormat(String tf, String words) {
		if (tf.equals("ISO8601")) {
			return  words;
		} else if (tf.equals("timestamps")) {
			return words;
		} else {
			SimpleDateFormat sdf = new SimpleDateFormat(tf);
			Date date = null;
			try {
				date = sdf.parse(words);

			} catch (java.text.ParseException e) {
				LOGGER.debug("input time format error please re-input, Unparseable date: " + tf);
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
		try {
			statement = connection.createStatement();
			BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream(new File(filename))));
			String line = "";
			String[] str_headInfo = br.readLine().split(",");
			deviceToColumn = new HashMap<String, ArrayList<Integer>>();
			colInfo = new ArrayList<String>();
			headInfo = new ArrayList<String>();
			
			if(str_headInfo.length <= 1) {
				LOGGER.debug("The CSV file illegal");
				System.exit(1);
			}
			
			timeseriesToType = new HashMap<String, String>();
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			
			
			for(int i = 1; i < str_headInfo.length; i++) {
				ResultSet resultSet = null;
				try {
					resultSet = databaseMetaData.getColumns(null, null, str_headInfo[i], null);
					while(resultSet.next()){
						timeseriesToType.put(resultSet.getString(0), resultSet.getString(1));
					}
				}catch(SQLException colerr) {
					LOGGER.debug("database Cannot find " +  str_headInfo[i] + ",stop import!");
					System.exit(1);
				}
			}
			
			for (int i = 1; i < str_headInfo.length; i++) {
				
				headInfo.add(str_headInfo[i]);
				String deviceInfo = str_headInfo[i].substring(0, str_headInfo[i].lastIndexOf("."));
				if (deviceToColumn.containsKey(deviceInfo)) {
					// storage every  device's sensor index info
					deviceToColumn.get(deviceInfo).add(i - 1); 
					colInfo.add(str_headInfo[i].substring(str_headInfo[i].lastIndexOf(".") + 1));
				} else {
					deviceToColumn.put(deviceInfo, new ArrayList<Integer>());
					// storage every device's sensor index info
					deviceToColumn.get(deviceInfo).add(i - 1); 
					colInfo.add(str_headInfo[i].substring(str_headInfo[i].lastIndexOf(".") + 1));
				}
			}
			while ((line = br.readLine()) != null) {
				ArrayList<String> sqls = createInsertSQL(line);
				for (String str : sqls) {
					try {
						statement.execute(str);
						LOGGER.debug( str + " :excuted successfull");

					} catch (SQLException e) {
						LOGGER.error(str + " :excuted fail");
					}
				}

			}
			br.close();
			statement.close();
		} catch (FileNotFoundException e) {
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
		}
	}

	private static String typeIdentify(String key_timeseries) {
		return timeseriesToType.get(key_timeseries);

	}
	
//	/**
//	 * create corresponding of timeseries and type 
//	 * @throws SQLException
//	 */
//	private static void createType() throws SQLException {
//		timeseriesToType = new HashMap<String, String>();
//		DatabaseMetaData databaseMetaData = connection.getMetaData();
//		ResultSet resultSet = databaseMetaData.getColumns(null, null, "root.fit", null);
//		while(resultSet.next()){
//			timeseriesToType.put(resultSet.getString(0), resultSet.getString(1));
//		}
////		timeseriesToType.put("root.fit.d1.s1", "INT32");
////		timeseriesToType.put("root.fit.d1.s2", "BYTE_ARRAY");
////		timeseriesToType.put("root.fit.d2.s1", "INT32");
////		timeseriesToType.put("root.fit.d2.s3", "INT32");
////		timeseriesToType.put("root.fit.p.s1", "INT32");
//	}
	
	/**
	 * create Insert SQL statement according to every line csv data
	 * @param line csv line data
	 * @return ArrayList<String> SQL statement list
	 */
	private static ArrayList<String> createInsertSQL(String line) {
		String[] words = line.split(",", headInfo.size() + 1);

		ArrayList<String> sqls = new ArrayList<String>();
		Iterator<Map.Entry<String, ArrayList<Integer>>> it = deviceToColumn.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, ArrayList<Integer>> entry = it.next();
			StringBuilder sbd = new StringBuilder();
			ArrayList<Integer> colIndex = entry.getValue();
			sbd.append("insert into " + entry.getKey() + "(timestamp");
			int skipcount = 0;
			for (int j = 0; j < colIndex.size(); ++j) {

				if (words[entry.getValue().get(j) + 1].equals("")) {
					skipcount++;
					continue;
				}
				sbd.append(", " + colInfo.get(colIndex.get(j)));
			}
			if (skipcount == entry.getValue().size())
				continue;

			String str_timestamps = setTimeFormat(timeformat, words[0]);
			if (str_timestamps == "")
				continue;
			sbd.append(") values(" + str_timestamps);

			for (int j = 0; j < colIndex.size(); ++j) {
				if (words[entry.getValue().get(j) + 1].equals(""))
					continue;
				if (typeIdentify(headInfo.get(colIndex.get(j))) == "BYTE_ARRAY") {
					sbd.append(", \'" + words[colIndex.get(j) + 1] + "\'");
				} else {
					sbd.append("," + words[colIndex.get(j) + 1]);
				}
			}
			sbd.append(")");
			sqls.add(sbd.toString());
		}

		return sqls;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

		Options options = createOptions();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
		CommandLine commandLine = null;
		CommandLineParser parser = new DefaultParser();
		Scanner scanner = new Scanner(System.in);

		if (args == null || args.length == 0) {
			hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
			return;
		}

		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption(HELP_ARGS)) {
				hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
				return;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		try {
			host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine);
			port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine);
			username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine);
			password = commandLine.getOptionValue(PASSWORD_ARGS);
			if (password == null) {
				System.out.print(TSFILEDB_CLI_PREFIX + "> please input password: ");
				password = scanner.nextLine();
			}
			filename = checkRequiredArg(FILE_ARGS, FILE_NAME, commandLine);
			timeformat = checkRequiredArg(TIMEFORMAT_ARGS, TIMEFORMAT_NAME, commandLine);
		} catch (ArgsErrorException e) {
			e.printStackTrace();
		}

		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);

		loadDataFromCSV();

		connection.close();
		scanner.close();

	}
}



