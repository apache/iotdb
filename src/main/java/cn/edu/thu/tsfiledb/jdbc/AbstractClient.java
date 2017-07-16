package cn.edu.thu.tsfiledb.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;

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
	protected static boolean timeFormatInISO8601 = true;

	protected static final String MAX_PRINT_ROW_COUNT_ARGS = "maxPRC";
	protected static final String MAX_PRINT_ROW_COUNT_NAME = "maxPrintRowCount";
	protected static int maxPrintRowCount = 1000;

	protected static final String TSFILEDB_CLI_PREFIX = "TsFileDB";
	protected static final String QUIT_COMMAND = "quit";
	protected static final String EXIT_COMMAND = "exit";
	protected static final String SHOW_METADATA_COMMAND = "show timeseries";
	protected static final int MAX_HELP_CONSOLE_WIDTH = 88;

	protected static final String[] AGGREGATION_OPERATOR = new String[] { "count", "min_timestamp", "max_timestamp",
			"max_value", "min_value" };

	protected static final String TIMESTAMP_STR = "Timestamp";
	protected static int maxTimeLength = 30;
	protected static int maxValueLength = 15;
	protected static String formatTime = "%" + maxTimeLength + "s|";
	protected static String formatValue = "%" + maxValueLength + "s|";
	
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
	
	protected static void output(ResultSet res, boolean printToConsole, String statement) {
		try {
			int cnt = 0;
			int colCount = res.getMetaData().getColumnCount();

			boolean printTimestamp = printTimestamp(statement);
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
						System.out.printf(formatValue, String.valueOf(res.getString(i)));
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
			System.out.println(TSFILEDB_CLI_PREFIX + "> record number = " + cnt);
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

		Option host = OptionBuilder.withArgName(HOST_NAME).hasArg().withDescription("Host Name (required)")
				.create(HOST_ARGS);
		options.addOption(host);

		Option port = OptionBuilder.withArgName(PORT_NAME).hasArg().withDescription("Port (required)")
				.create(PORT_ARGS);
		options.addOption(port);

		Option username = OptionBuilder.withArgName(USERNAME_NAME).hasArg().withDescription("User name (required)")
				.create(USERNAME_ARGS);
		options.addOption(username);

		Option password = OptionBuilder.withArgName(PASSWORD_NAME).hasArg().withDescription("password (optional)")
				.create(PASSWORD_ARGS);
		options.addOption(password);

		Option maxPrintCount = OptionBuilder.withArgName(MAX_PRINT_ROW_COUNT_NAME).hasArg()
				.withDescription("Maximum number of rows displayed (optional)").create(MAX_PRINT_ROW_COUNT_ARGS);
		options.addOption(maxPrintCount);
		return options;
	}

	private static String formatDatetime(long timestamp) {
		if (timeFormatInISO8601) {
			DateTime dateTime = new DateTime(timestamp);
			return dateTime.toString();
		}
		return timestamp + "";
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

	protected static void setTimeFormatForNumber() {
		timeFormatInISO8601 = false;
		maxTimeLength = maxValueLength;
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

	protected static boolean printTimestamp(String statement) {
		if (statement != null) {
			for (String operator : AGGREGATION_OPERATOR) {
				if (statement.indexOf(operator) != -1) {
					return false;
				}
			}
		}
		return true;
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
	
}
