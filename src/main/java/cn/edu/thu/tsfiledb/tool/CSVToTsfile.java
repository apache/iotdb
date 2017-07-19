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
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;

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

	private static final String STRING_DATA_TYPE = "BYTE_ARRAY";

	private static String host;
	private static String port;
	private static String username;
	private static String password;
	private static String filename;
	private static String timeformat = "timestamps";

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
			throw new ArgsErrorException(msg);
		}
		return str;
	}

	/**
	 * 
	 * @param tf time format, optioned:ISO8601, timestamps, self-defined:
	 *            yyyy-mm-dd hh:mm:ss
	 * @param timestamp the time column of the csv file
	 * @return the timestamps will insert into SQL
	 */
	private static String setTimeFormat(String tf, String timestamp) {
		if (tf.equals("ISO8601")) {
				try {
					Long.parseLong(timestamp);									
				}catch (NumberFormatException nfe) {
					return timestamp;
				}
			return  "";
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
		try {
			Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
			connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
			br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filename))));
			File file = new File("csvInsertError.txt");
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
					br.close();
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
			while ((line = br.readLine()) != null) {
				ArrayList<String> sqls = createInsertSQL(line);
				for (String str : sqls) {
					try {
						statement.execute(str);
						LOGGER.debug("{} :excuted successful!", str);
					} catch (SQLException e) {
						LOGGER.error("{} :excuted fail!", str);
						bw.write(e.getMessage());
						bw.newLine();
					}
				}
			}
			if(file.length() == 0) {
				file.delete();
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

	}

	private static String typeIdentify(String timeseries) {
		return timeseriesToType.get(timeseries);

	}

	/**
	 * create Insert SQL statement according to every line csv data
	 * 
	 * @param line
	 *            csv line data
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
			// define every device null value' number, if the number equal the
			// sensor number, the insert operation stop
			if (skipcount == entry.getValue().size())
				continue;

			String timestampsStr = setTimeFormat(timeformat, words[0]);
			if (timestampsStr == "") {
				LOGGER.error("Time Format Error!");
				continue;
			}
			sbd.append(") values(").append(timestampsStr);

			for (int j = 0; j < colIndex.size(); ++j) {
				if (words[entry.getValue().get(j) + 1].equals(""))
					continue;
				if (typeIdentify(headInfo.get(colIndex.get(j))) == STRING_DATA_TYPE) {
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
			if (commandLine.hasOption(HELP_ARGS)) {
				hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
				scanner.close();
				return;
			}
			host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine);
			port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine);
			username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine);
			password = commandLine.getOptionValue(PASSWORD_ARGS);
			if (password == null) {
				System.out.print(TSFILEDB_CLI_PREFIX + "> please input password: ");
				password = scanner.nextLine();
			}
			filename = checkRequiredArg(FILE_ARGS, FILE_NAME, commandLine);
		} catch (ParseException e) {
			LOGGER.error("problems encountered while parsing the command line tokens.", e);
			scanner.close();
			System.exit(1);
		} catch (ArgsErrorException e) {
			hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
			scanner.close();
			System.exit(1);
		}
		try {
			timeformat = checkRequiredArg(TIMEFORMAT_ARGS, TIMEFORMAT_NAME, commandLine);
		} catch (ArgsErrorException e) {
			timeformat = "timestamps";
			System.out.println("time format use default value long type!");
		}

		loadDataFromCSV();
	}
}
