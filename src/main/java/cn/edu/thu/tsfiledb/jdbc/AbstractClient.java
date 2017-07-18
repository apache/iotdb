package cn.edu.thu.tsfiledb.jdbc;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
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
	protected static final String TIME_KEY_WORD = "time";

	protected static final String MAX_PRINT_ROW_COUNT_ARGS = "maxPRC";
	protected static final String MAX_PRINT_ROW_COUNT_NAME = "maxPrintRowCount";
	protected static int maxPrintRowCount = 1000;

	protected static final String SET_TIMESTAMP_DISPLAY = "set time_display_type";

	protected static final String TSFILEDB_CLI_PREFIX = "TsFileDB";
	private static final String QUIT_COMMAND = "quit";
	private static final String EXIT_COMMAND = "exit";
	private static final String SHOW_METADATA_COMMAND = "show timeseries";
	protected static final int MAX_HELP_CONSOLE_WIDTH = 88;

	protected static final String TIMESTAMP_STR = "Time";
	protected static final int ISO_DATETIME_LEN = 30;
	protected static int maxTimeLength = ISO_DATETIME_LEN;
	protected static int maxValueLength = 15;
	protected static String formatTime = "%" + maxTimeLength + "s|";
	protected static String formatValue = "%" + maxValueLength + "s|";

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
	}
	
	public static void output(ResultSet res, boolean printToConsole, String statement) {
		try {
			int cnt = 0;
			ResultSetMetaData resultSetMetaData = res.getMetaData();
			int colCount = resultSetMetaData.getColumnCount();
			boolean printTimestamp = res.getMetaData().getColumnTypeName(0) == null ? true : false; 
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
						System.out.printf(formatTime, formatDatetime(res.getLong(TIMESTAMP_STR)));
					}
				}

				for (int i = 1; i < colCount; i++) {
					if (printToConsole && cnt < maxPrintRowCount) {
					    	if(resultSetMetaData.getColumnLabel(i).indexOf(TIME_KEY_WORD) != -1){
					    	    	System.out.printf(formatValue, formatDatetime(res.getLong(i)));
					    	} else{
							System.out.printf(formatValue, String.valueOf(res.getString(i)));
					    	}
					}
				}

				if (printToConsole && cnt < maxPrintRowCount) {
					System.out.printf("\n");
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
				printHeader = true;
			}
			printBlockLine(printTimestamp, colCount, res);
			System.out.println("record number = " + cnt);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
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

	private static String formatDatetime(long timestamp) {
		DateTime dateTime = new DateTime(timestamp);
		switch (timeFormat) {
		case "long":
		case "number":
			return timestamp+"";
		case "default":
		case "ISO8601":
			return dateTime.toString();
		default:
			return dateTime.toString(timeFormat);
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
		switch (timeFormat) {
		case "long":
		case "number":
			maxTimeLength = maxValueLength;
			break;
		case "default":
		case "ISO8601":
			maxTimeLength = ISO_DATETIME_LEN;
			break;
		default:
			maxTimeLength = TIMESTAMP_STR.length() > newTimeFormat.length() ? TIMESTAMP_STR.length() : newTimeFormat.length();
			break;
		}
		timeFormat = newTimeFormat;
		formatTime = "%" + maxTimeLength + "s|";
	}

	protected static void printBlockLine(boolean printTimestamp, int colCount, ResultSet res) throws SQLException {
		StringBuilder blockLine = new StringBuilder();
		for (int i = 0; i < colCount - 1; i++) {
			int len = res.getMetaData().getColumnLabel(i + 1).length();
			maxValueLength = maxValueLength < len ? len : maxValueLength;
		}
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
		for (int i = 0; i < colCount - 1; i++) {
			System.out.printf(formatValue, res.getMetaData().getColumnLabel(i + 1));
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
		System.out.println(" _______________________________.___.__          \n"
				+ " \\__    ___/   _____/\\_   _____/|   |  |   ____  \n"
				+ "   |    |  \\_____  \\  |    __)  |   |  | _/ __ \\ \n"
				+ "   |    |  /        \\ |     \\   |   |  |_\\  ___/ \n"
				+ "   |____| /_______  / \\___  /   |___|____/\\___  >   version 0.0.1\n"
				+ "                  \\/      \\/                  \\/ \n");
	}

	protected static OPERATION_RESULT handleInputInputCmd(String cmd, Connection connection){
		String specialCmd = cmd.toLowerCase().trim();

		if (specialCmd.equals(QUIT_COMMAND) || specialCmd.equals(EXIT_COMMAND)) {
			System.out.println(specialCmd + " normally");
			return OPERATION_RESULT.RETURN_OPER;
		}
		if (specialCmd.equals(SHOW_METADATA_COMMAND)) {
			try {
				System.out.println(connection.getMetaData());
			} catch (SQLException e) {
				System.out.println("> failed to show timeseries because: " + e.getMessage());
			}
			return OPERATION_RESULT.CONTINUE_OPER;
		}

		if(specialCmd.startsWith(SET_TIMESTAMP_DISPLAY)){
			String[] values = specialCmd.split("=");
			if(values.length != 2){
				System.out.println(String.format("%s> format error, please input like set %s=ISO8601", TSFILEDB_CLI_PREFIX, SET_TIMESTAMP_DISPLAY));
				return OPERATION_RESULT.CONTINUE_OPER;
			}
			setTimeFormat(values[1]);
			System.out.println("time display type has set to "+values[1]);
			return OPERATION_RESULT.CONTINUE_OPER;
		}
		Statement statement = null;
		try {
			statement = connection.createStatement();
			boolean hasResultSet = statement.execute(cmd.trim());
			if (hasResultSet) {
				ResultSet resultSet = statement.getResultSet();
				output(resultSet, printToConsole, cmd.trim());
			}
			System.out.println("execute successfully.");
		} catch (TsfileSQLException e) {
			System.out.println("statement error: " + e.getMessage());
		} catch (Exception e) {
			System.out.println("connection error: " + e.getMessage());
		} finally {
		    	if(statement != null){
			    	try {
				    statement.close();
				} catch (SQLException e) {
				    System.out.println("cannot close statement because: " + e.getMessage());
				}
		    	}
		}
		return OPERATION_RESULT.NO_OPER;
	}

	enum OPERATION_RESULT{
		RETURN_OPER, CONTINUE_OPER, NO_OPER
	}
}
