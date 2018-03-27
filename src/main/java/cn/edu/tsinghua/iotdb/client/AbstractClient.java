package cn.edu.tsinghua.iotdb.client;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.tool.ImportCsv;

import cn.edu.tsinghua.iotdb.jdbc.TsfileConnection;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractClient {
	protected static final String HOST_ARGS = "h";
	protected static final String HOST_NAME = "host";

	protected static final String HELP_ARGS = "help";

	protected static final String PORT_ARGS = "p";
	protected static final String PORT_NAME = "port";

	protected static final String PASSWORD_ARGS = "pw";
	protected static final String PASSWORD_NAME = "password";

	protected static final String USERNAME_ARGS = "u";
	protected static final String USERNAME_NAME = "username";

	protected static final String ISO8601_ARGS = "disableISO8601";
	protected static String timeFormat = "default";
//	protected static final String TIME_KEY_WORD = "time";
	protected static final List<String> AGGREGRATE_TIME_LIST = new ArrayList<>();
	
	protected static final String MAX_PRINT_ROW_COUNT_ARGS = "maxPRC";
	protected static final String MAX_PRINT_ROW_COUNT_NAME = "maxPrintRowCount";
	
	protected static final String SET_MAX_DISPLAY_NUM = "set max_display_num";
	protected static int maxPrintRowCount = 1000;

	protected static final String SET_TIMESTAMP_DISPLAY = "set time_display_type";
	protected static final String SHOW_TIMESTAMP_DISPLAY = "show time_display_type";
	protected static final String SET_TIME_ZONE = "set time_zone";
	protected static final String SHOW_TIMEZONE = "show time_zone";
	
	protected static final String SET_FETCH_SIZE = "set fetch_size";
	protected static final String SHOW_FETCH_SIZE = "show fetch_size";
	protected static int fetchSize = 10000;
	
	protected static final String TSFILEDB_CLI_PREFIX = "IoTDB";
	private static final String QUIT_COMMAND = "quit";
	private static final String EXIT_COMMAND = "exit";
	private static final String SHOW_METADATA_COMMAND = "show timeseries";
	protected static final int MAX_HELP_CONSOLE_WIDTH = 88;

	protected static final String TIMESTAMP_STR = "Time";
	protected static final int ISO_DATETIME_LEN = 23;
	protected static int maxTimeLength = ISO_DATETIME_LEN;
	protected static int maxValueLength = 15;
	protected static String formatTime = "%" + maxTimeLength + "s|";
	protected static String formatValue = "%" + maxValueLength + "s|";
	
	protected static final String IMPORT_CMD = "import";
	protected static final String EXPORT_CMD = "export";
	
	private static final String NEED_NOT_TO_PRINT_TIMESTAMP = "AGGREGATION";
	
	protected static String host;
	protected static String port;
	protected static String username;
	protected static String password;

	protected static boolean printToConsole = true;

	protected static Set<String> keywordSet = new HashSet<>();

	protected static void init(){
		keywordSet.add("-"+HOST_ARGS);
		keywordSet.add("-"+HELP_ARGS);
		keywordSet.add("-"+PORT_ARGS);
		keywordSet.add("-"+PASSWORD_ARGS);
		keywordSet.add("-"+USERNAME_ARGS);
		keywordSet.add("-"+ISO8601_ARGS);
		keywordSet.add("-"+MAX_PRINT_ROW_COUNT_ARGS);
		
		AGGREGRATE_TIME_LIST.add(AggregationConstant.MAX_TIME);
		AGGREGRATE_TIME_LIST.add(AggregationConstant.MIN_TIME);
	}

	public static void output(ResultSet res, boolean printToConsole, String statement, DateTimeZone timeZone) throws SQLException {
		int cnt = 0;
		int displayCnt = 0;
		ResultSetMetaData resultSetMetaData = res.getMetaData();
		int colCount = resultSetMetaData.getColumnCount();
		boolean printTimestamp = true;
		if (res.getMetaData().getColumnTypeName(0) != null) {
			printTimestamp = !res.getMetaData().getColumnTypeName(0).toUpperCase().equals(NEED_NOT_TO_PRINT_TIMESTAMP);
		}
		boolean printHeader = false;

		// Output values
		while (res.next()) {

			// Output Labels
			if (!printHeader) {
				printBlockLine(printTimestamp, colCount, res);
				printName(printTimestamp, colCount, res);
				printBlockLine(printTimestamp, colCount, res);
				printHeader = true;
			}
			if (cnt < maxPrintRowCount) {
				System.out.print("|");
				if (printTimestamp) {
					System.out.printf(formatTime, formatDatetime(res.getLong(TIMESTAMP_STR), timeZone));
				}
			}

			for (int i = 2; i <= colCount; i++) {
				if (printToConsole && cnt < maxPrintRowCount) {
					boolean flag = false;
					for(String timeStr : AGGREGRATE_TIME_LIST) {
						if(resultSetMetaData.getColumnLabel(i).toUpperCase().indexOf(timeStr.toUpperCase()) != -1){
					 		flag = true;
					 		break;
					 	}
					}
					if (flag) {
						try {
							System.out.printf(formatValue, formatDatetime(res.getLong(i), timeZone));
						} catch (Exception e) {
							System.out.printf(formatValue, "null");
						}
					} else {
						System.out.printf(formatValue, String.valueOf(res.getString(i)));
					}
				}
			}

			if (printToConsole && cnt < maxPrintRowCount) {
				System.out.printf("\n");
				displayCnt++;
			}
			cnt++;

			if (!printToConsole && cnt % 10000 == 0) {
				System.out.println(cnt);
			}
		}
		if (!printHeader) {
			printBlockLine(printTimestamp, colCount, res);
			printName(printTimestamp, colCount, res);
			printBlockLine(printTimestamp, colCount, res);
		}
		printBlockLine(printTimestamp, colCount, res);
		System.out.println(String.format("Display the first %s lines", displayCnt));
		System.out.println(StringUtils.repeat('-', 40));
		System.out.println("Total line number = " + cnt);
	}

	protected static Options createOptions() {
		Options options = new Options();
		Option help = new Option(HELP_ARGS, false, "Display help information");
		help.setRequired(false);
		options.addOption(help);

		Option timeFormat = new Option(ISO8601_ARGS, false, "Display timestamp in number");
		timeFormat.setRequired(false);
		options.addOption(timeFormat);

		Option host = Option.builder(HOST_ARGS).argName(HOST_NAME).hasArg().desc("Host Name (required)").build();
		options.addOption(host);

		Option port = Option.builder(PORT_ARGS).argName(PORT_NAME).hasArg().desc("Port (required)").build();
		options.addOption(port);

		Option username = Option.builder(USERNAME_ARGS).argName(USERNAME_NAME).hasArg().desc("User name (required)").build();
		options.addOption(username);

		Option password = Option.builder(PASSWORD_ARGS).argName(PASSWORD_NAME).hasArg().desc("password (optional)").build();
		options.addOption(password);

		Option maxPrintCount = Option.builder(MAX_PRINT_ROW_COUNT_ARGS).argName(MAX_PRINT_ROW_COUNT_NAME).hasArg()
				.desc("Maximum number of rows displayed (optional)").build();
		options.addOption(maxPrintCount);
		return options;
	}

	private static String formatDatetime(long timestamp, DateTimeZone timeZone) {
		switch (timeFormat) {
		case "long":
		case "number":
			return timestamp+"";
		case "default":
		case "iso8601":
			return new DateTime(timestamp, timeZone).toString(ISODateTimeFormat.dateHourMinuteSecondMillis());
		default:
			return new DateTime(timestamp, timeZone).toString(timeFormat);
		}
	}

	protected static String checkRequiredArg(String arg, String name, CommandLine commandLine) throws ArgsErrorException {
		String str = commandLine.getOptionValue(arg);
		if (str == null) {
			String msg = String.format("%s: Required values for option '%s' not provided", TSFILEDB_CLI_PREFIX, name);
			System.out.println(msg);
			System.out.println("Use -help for more information");
			throw new ArgsErrorException(msg);
		}
		return str;
	}

	protected static void setTimeFormat(String newTimeFormat) {
		switch (newTimeFormat.trim().toLowerCase()) {
		case "long":
		case "number":
			maxTimeLength = maxValueLength;
			timeFormat = newTimeFormat.trim().toLowerCase();
			break;
		case "default":
		case "iso8601":
			maxTimeLength = ISO_DATETIME_LEN;
			timeFormat = newTimeFormat.trim().toLowerCase();
			break;
		default:
			// use java default SimpleDateFormat to check whether input time format is legal
			// if illegal, it will throw an exception
			new SimpleDateFormat(newTimeFormat.trim());
			maxTimeLength = TIMESTAMP_STR.length() > newTimeFormat.length() ? TIMESTAMP_STR.length() : newTimeFormat.length();
			timeFormat = newTimeFormat;
			break;
		}
		formatTime = "%" + maxTimeLength + "s|";
	}
	
	private static void setFetchSize(String fetchSizeString){
		fetchSize = Integer.parseInt(fetchSizeString.trim());
	}
	
	protected static void setMaxDisplayNumber(String maxDisplayNum){
		maxPrintRowCount = Integer.parseInt(maxDisplayNum.trim());
		if (maxPrintRowCount < 0) {
			maxPrintRowCount = Integer.MAX_VALUE;
		}
	}

	protected static void printBlockLine(boolean printTimestamp, int colCount, ResultSet res) throws SQLException {
		StringBuilder blockLine = new StringBuilder();
		int tmp = Integer.MIN_VALUE;
		for (int i = 1; i <= colCount; i++) {
			int len = res.getMetaData().getColumnLabel(i).length();
			tmp = tmp > len ? tmp : len;
		}
		maxValueLength = tmp;
		if (printTimestamp) {
			blockLine.append("+").append(StringUtils.repeat('-', maxTimeLength)).append("+");
		} else {
			blockLine.append("+");
		}
		for (int i = 0; i < colCount - 1; i++) {
			blockLine.append(StringUtils.repeat('-', maxValueLength)).append("+");
		}
		System.out.println(blockLine);
	}

	protected static void printName(boolean printTimestamp, int colCount, ResultSet res) throws SQLException {
		System.out.print("|");
		formatValue = "%" + maxValueLength + "s|";
		if (printTimestamp) {
			System.out.printf(formatTime, TIMESTAMP_STR);
		}
		for (int i = 2; i <= colCount; i++) {
			System.out.printf(formatValue, res.getMetaData().getColumnLabel(i));
		}
		System.out.printf("\n");
	}

	protected static String[] checkPasswordArgs(String[] args) {
		int index = -1;
		for(int i = 0; i < args.length; i++){
			if(args[i].equals("-"+PASSWORD_ARGS)){
				index = i;
				break;
			}
		}
		if(index > 0){
			if((index+1 >= args.length) || (index+1 < args.length && keywordSet.contains(args[index+1]))){
				return ArrayUtils.remove(args, index);
			}
		}
		return args;
	}

	protected static void displayLogo(){
		System.out.println(
				" _____       _________  ______   ______    \n" +
				"|_   _|     |  _   _  ||_   _ `.|_   _ \\   \n" +
				"  | |   .--.|_/ | | \\_|  | | `. \\ | |_) |  \n" +
				"  | | / .'`\\ \\  | |      | |  | | |  __'.  \n" +
				" _| |_| \\__. | _| |_    _| |_.' /_| |__) | \n" +
				"|_____|'.__.' |_____|  |______.'|_______/  version "+TsFileDBConstant.VERSION+"\n" +
				"                                           \n");
	}

	protected static OPERATION_RESULT handleInputInputCmd(String cmd, TsfileConnection connection){
		String specialCmd = cmd.toLowerCase().trim();

		if (specialCmd.equals(QUIT_COMMAND) || specialCmd.equals(EXIT_COMMAND)) {
			System.out.println(specialCmd + " normally");
			return OPERATION_RESULT.RETURN_OPER;
		}
		if (specialCmd.equals(SHOW_METADATA_COMMAND)) {
			try {
				System.out.println(connection.getMetaData());
			} catch (SQLException e) {
				System.out.println("Failed to show timeseries because: " + e.getMessage());
			}
			return OPERATION_RESULT.CONTINUE_OPER;
		}

		if(specialCmd.startsWith(SET_TIMESTAMP_DISPLAY)){
			String[] values = specialCmd.split("=");
			if(values.length != 2){
				System.out.println(String.format("Time display format error, please input like %s=ISO8601", SET_TIMESTAMP_DISPLAY));
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			try {
				setTimeFormat(cmd.split("=")[1]);
			} catch (Exception e) {
				System.out.println(String.format("time display format error, %s", e.getMessage()));
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			System.out.println("Time display type has set to "+cmd.split("=")[1].trim());
			return OPERATION_RESULT.CONTINUE_OPER;
		}
		
		if(specialCmd.startsWith(SET_TIME_ZONE)){
			String[] values = specialCmd.split("=");
			if(values.length != 2){
				System.out.println(String.format("Time zone format error, please input like %s=+08:00", SET_TIME_ZONE));
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			try {
				connection.setTimeZone(cmd.split("=")[1].trim());
			} catch (Exception e) {
				System.out.println(String.format("Time zone format error, %s", e.getMessage()));
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			System.out.println("Time zone has set to "+values[1].trim());
			return OPERATION_RESULT.CONTINUE_OPER;
		}
		
		if(specialCmd.startsWith(SET_FETCH_SIZE)){
			String[] values = specialCmd.split("=");
			if(values.length != 2){
				System.out.println(String.format("Fetch size format error, please input like %s=10000", SET_FETCH_SIZE));
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			try {
				setFetchSize(cmd.split("=")[1]);
			} catch (Exception e) {
				System.out.println(String.format("Fetch size format error, %s", e.getMessage()));
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			System.out.println("Fetch size has set to "+values[1].trim());
			return OPERATION_RESULT.CONTINUE_OPER;
		}

		if(specialCmd.startsWith(SET_MAX_DISPLAY_NUM)) {
			String[] values = specialCmd.split("=");
			if(values.length != 2){
				System.out.println(String.format("Max display number format error, please input like %s = 10000", SET_MAX_DISPLAY_NUM));
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			try {
				setMaxDisplayNumber(cmd.split("=")[1]);
			} catch (Exception e) {
				System.out.println(String.format("Max display number format error, %s", e.getMessage()));
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			System.out.println("Max display number has set to "+values[1].trim());
			return OPERATION_RESULT.CONTINUE_OPER;
		}

		if(specialCmd.startsWith(SHOW_TIMEZONE)){
			try {
				System.out.println("Current time zone: "+connection.getTimeZone());
			} catch (Exception e) {
				System.out.println("Cannot get time zone from server side because: "+e.getMessage());
			}
			return OPERATION_RESULT.CONTINUE_OPER;
		}
		if(specialCmd.startsWith(SHOW_TIMESTAMP_DISPLAY)){
			System.out.println("Current time format: "+timeFormat);
			return OPERATION_RESULT.CONTINUE_OPER;
		}
		if(specialCmd.startsWith(SHOW_FETCH_SIZE)){
			System.out.println("Current fetch size: "+fetchSize);
			return OPERATION_RESULT.CONTINUE_OPER;
		}
		
		if(specialCmd.startsWith(IMPORT_CMD)){
			String[] values = specialCmd.split(" ");
			if(values.length != 2){
				System.out.println(String.format("Please input like: import /User/myfile. Noted that your file path cannot contain any space character)"));
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			try {
				System.out.println(cmd.split(" ")[1]);
				ImportCsv.importCsvFromFile(host, port, username, password, cmd.split(" ")[1], connection.getTimeZone());
			} catch (SQLException e) {
				System.out.println(String.format("Failed to import from %s because %s", cmd.split(" ")[1], e.getMessage()));
			} catch (TException e) {
				System.out.println("Cannot connect to server");
			}
			return OPERATION_RESULT.CONTINUE_OPER;
		}

		Statement statement = null;
		long startTime = System.currentTimeMillis();
		try {
			DateTimeZone timeZone = DateTimeZone.forID(connection.getTimeZone());
			statement = connection.createStatement();
			statement.setFetchSize(fetchSize);
			boolean hasResultSet = statement.execute(cmd.trim());
			if (hasResultSet) {
				ResultSet resultSet = statement.getResultSet();
				output(resultSet, printToConsole, cmd.trim(), timeZone);
			}
			System.out.println("Execute successfully.");
		} catch (Exception e) {
			System.out.println("Msg: " + e.getMessage());
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					System.out.println("Cannot close statement because: " + e.getMessage());
				}
			}
		}
		long costTime = System.currentTimeMillis() - startTime;
		System.out.println(String.format("It costs %.3fs", costTime/1000.0));
		return OPERATION_RESULT.NO_OPER;
	}

	enum OPERATION_RESULT{
		RETURN_OPER, CONTINUE_OPER, NO_OPER
	}
}
