package cn.edu.tsinghua.iotdb.client;

import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.jdbc.TsfileDatabaseMetadata;
import cn.edu.tsinghua.iotdb.jdbc.TsfileMetadataResultSet;
import cn.edu.tsinghua.iotdb.tool.ImportCsv;
import cn.edu.tsinghua.service.rpc.thrift.ServerProperties;
import cn.edu.tsinghua.iotdb.jdbc.TsfileConnection;

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

	protected static final String IOTDB_CLI_PREFIX = "IoTDB";
	protected static final String SCRIPT_HINT = "./start-client.sh(start-client.bat if Windows)";
	protected static final String QUIT_COMMAND = "quit";
	protected static final String EXIT_COMMAND = "exit";
	protected static final String SHOW_METADATA_COMMAND = "show timeseries";
	protected static final int MAX_HELP_CONSOLE_WIDTH = 88;

	protected static final String TIMESTAMP_STR = "Time";
	protected static final int ISO_DATETIME_LEN = 23;
	protected static int maxTimeLength = ISO_DATETIME_LEN;
	protected static int maxValueLength = 15;
	/**
	 * control the width of columns for 'show timeseries <path>' and 'show storage group'
	 * <p> for 'show timeseries <path>':
	 * <table>
	 * <tr> <th>Timeseries (width:75)</th> <th>Storage Group (width:45)</th> <th>DataType width:8)</th> <th>Encoding (width:8)</th></tr>
	 * <tr> <td>root.vehicle.d1.s1</td>    <td>root.vehicle</td>             <td>INT32</td>             <td>PLAIN</td></tr>
	 * <tr> <td>...</td>                   <td>...</td>                      <td>...</td>               <td>...</td></tr>
	 * </table>
	 * </p>
	 * <p> for show storage group <path>':
	 * <table>
	 * <tr> <th>STORAGE_GROUP (width:75)</th> </tr>
	 * <tr> <td>root.vehicle</td>             </tr>
	 * <tr> <td>...</td>                      </tr>
	 * </table>
	 * </p>
	 */
    protected static int[] maxValueLengthForShow = new int[]{75, 45, 8, 8};
	protected static String formatTime = "%" + maxTimeLength + "s|";
	protected static String formatValue = "%" + maxValueLength + "s|";

	protected static int DIVIDING_LINE_LEN = 40;

	protected static final String IMPORT_CMD = "import";
	protected static final String EXPORT_CMD = "export";

	private static final String NEED_NOT_TO_PRINT_TIMESTAMP = "AGGREGATION";

	protected static String host = "127.0.0.1";
	protected static String port = "6667";
	protected static String username;
	protected static String password;

	protected static boolean printToConsole = true;

	protected static Set<String> keywordSet = new HashSet<>();
	
	protected static ServerProperties properties = null;

	protected static void init(){
		keywordSet.add("-"+HOST_ARGS);
		keywordSet.add("-"+HELP_ARGS);
		keywordSet.add("-"+PORT_ARGS);
		keywordSet.add("-"+PASSWORD_ARGS);
		keywordSet.add("-"+USERNAME_ARGS);
		keywordSet.add("-"+ISO8601_ARGS);
		keywordSet.add("-"+MAX_PRINT_ROW_COUNT_ARGS);
	}

	public static void output(ResultSet res, boolean printToConsole, String statement, DateTimeZone timeZone) throws SQLException {
		int cnt = 0;
		int displayCnt = 0;
		boolean printTimestamp = true;
		boolean printHeader = false;
		ResultSetMetaData resultSetMetaData = res.getMetaData();

		int colCount = resultSetMetaData.getColumnCount();

		boolean isShow = res instanceof TsfileMetadataResultSet;
		if (!isShow && resultSetMetaData.getColumnTypeName(0) != null) {
			printTimestamp = !res.getMetaData().getColumnTypeName(0).toUpperCase().equals(NEED_NOT_TO_PRINT_TIMESTAMP);
		}

		// Output values
		while (res.next()) {
			// Output Labels
		    	if (printToConsole) {
				if (!printHeader) {
			    		printBlockLine(printTimestamp, colCount, resultSetMetaData, isShow);
			    		printName(printTimestamp, colCount, resultSetMetaData, isShow);
			    		printBlockLine(printTimestamp, colCount, resultSetMetaData, isShow);
			    		printHeader = true;
				}

				if (isShow) { // 'show timeseries <path>' or 'show storage group' metadata results
			    		System.out.print("|");
			    		for (int i = 1; i <= colCount; i++) {
						formatValue = "%" + maxValueLengthForShow[i - 1] + "s|";
						System.out.printf(formatValue, String.valueOf(res.getString(i)));
			    		}
			    		System.out.println();
				} else { // queried data results
			    		if (displayCnt < maxPrintRowCount) { // NOTE displayCnt only works on queried data results
						System.out.print("|");
						if (printTimestamp) {
				    			System.out.printf(formatTime, formatDatetime(res.getLong(TIMESTAMP_STR), timeZone));
						}
						for (int i = 2; i <= colCount; i++) {
				    			boolean flag = false;
				    			for (String timeStr : AGGREGRATE_TIME_LIST) {
								if (resultSetMetaData.getColumnLabel(i).toUpperCase().contains(timeStr.toUpperCase())) {
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
						System.out.println();
						displayCnt++;
					}
				}
			}

			cnt++;

			if (!printToConsole && cnt % 10000 == 0) {
				System.out.println(cnt);
			}
		}

		if (printToConsole) {
			if (!printHeader) {
				printBlockLine(printTimestamp, colCount, resultSetMetaData, isShow);
				printName(printTimestamp, colCount, resultSetMetaData, isShow);
				printBlockLine(printTimestamp, colCount, resultSetMetaData, isShow);
		    	} else {
				printBlockLine(printTimestamp, colCount, resultSetMetaData, isShow);
		    	}
		    	if (displayCnt == maxPrintRowCount) {
				System.out.println(String.format("Reach maxPrintRowCount = %s lines", maxPrintRowCount));
		    	}
		}

		System.out.println(StringUtils.repeat('-', DIVIDING_LINE_LEN));
		if (isShow) {
			int type = res.getType();
		    	if (type == TsfileMetadataResultSet.MetadataType.STORAGE_GROUP.ordinal()) { // storage group
				System.out.println("Total storage group number = " + cnt);
		    	} else if (type == TsfileMetadataResultSet.MetadataType.TIMESERIES.ordinal()) { // show timeseries <path>
				System.out.println("Total timeseries number = " + cnt);
		    	}
		} else {
		    	System.out.println("Total line number = " + cnt);
		}
	}

	protected static Options createOptions() {
		Options options = new Options();
		Option help = new Option(HELP_ARGS, false, "Display help information(optional)");
		help.setRequired(false);
		options.addOption(help);

		Option timeFormat = new Option(ISO8601_ARGS, false, "Display timestamp in number(optional)");
		timeFormat.setRequired(false);
		options.addOption(timeFormat);

		Option host = Option.builder(HOST_ARGS).argName(HOST_NAME).hasArg().desc("Host Name (optional, default 127.0.0.1)").build();
		options.addOption(host);

		Option port = Option.builder(PORT_ARGS).argName(PORT_NAME).hasArg().desc("Port (optional, default 6667)").build();
		options.addOption(port);

		Option username = Option.builder(USERNAME_ARGS).argName(USERNAME_NAME).hasArg().desc("User name (required)").required().build();
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

	protected static String checkRequiredArg(String arg, String name, CommandLine commandLine, boolean isRequired, String defaultValue) throws ArgsErrorException {
		String str = commandLine.getOptionValue(arg);
		if (str == null) {
			if(isRequired) {
				String msg = String.format("%s: Required values for option '%s' not provided", IOTDB_CLI_PREFIX, name);
				System.out.println(msg);
				System.out.println("Use -help for more information");
				throw new ArgsErrorException(msg);
			} else if (defaultValue == null){
				String msg = String.format("%s: Required values for option '%s' is null.", IOTDB_CLI_PREFIX, name);
				throw new ArgsErrorException(msg);
			} else {
				return defaultValue;
			}
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
		long tmp = Long.parseLong(fetchSizeString.trim());
		if(tmp > Integer.MAX_VALUE || tmp < 0) {
			fetchSize = Integer.MAX_VALUE;
		} else {
			fetchSize = Integer.parseInt(fetchSizeString.trim());
		}
	}

	protected static void setMaxDisplayNumber(String maxDisplayNum){
		long tmp = Long.parseLong(maxDisplayNum.trim());
		if(tmp > Integer.MAX_VALUE || tmp < 0) {
			maxPrintRowCount = Integer.MAX_VALUE;
		} else {
			maxPrintRowCount = Integer.parseInt(maxDisplayNum.trim());
		}
	}

	protected static void printBlockLine(boolean printTimestamp, int colCount, ResultSetMetaData resultSetMetaData, boolean isShowTs) throws SQLException {
		StringBuilder blockLine = new StringBuilder();
		if (isShowTs) {
			blockLine.append("+");
			for (int i = 1; i <= colCount; i++) {
				blockLine.append(StringUtils.repeat('-', maxValueLengthForShow[i - 1])).append("+");
			}
		} else {
			int tmp = Integer.MIN_VALUE;
			for (int i = 1; i <= colCount; i++) {
				int len = resultSetMetaData.getColumnLabel(i).length();
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
		}
		System.out.println(blockLine);
	}

	protected static void printName(boolean printTimestamp, int colCount, ResultSetMetaData resultSetMetaData, boolean isShowTs) throws SQLException {
		System.out.print("|");
		if (isShowTs) {
			for (int i = 1; i <= colCount; i++) {
				formatValue = "%" + maxValueLengthForShow[i - 1] + "s|";
				System.out.printf(formatValue, resultSetMetaData.getColumnName(i));
			}
		} else {
			formatValue = "%" + maxValueLength + "s|";
			if (printTimestamp) {
				System.out.printf(formatTime, TIMESTAMP_STR);
			}
			for (int i = 2; i <= colCount; i++) {
				System.out.printf(formatValue, resultSetMetaData.getColumnLabel(i));
			}
		}
		System.out.printf("\n");
	}

	protected static String[] removePasswordArgs(String[] args) {
		int index = -1;
		for(int i = 0; i < args.length; i++){
			if(args[i].equals("-"+PASSWORD_ARGS)){
				index = i;
				break;
			}
		}
		if(index >= 0){
			if((index+1 >= args.length) || (index+1 < args.length && keywordSet.contains(args[index+1]))){
				return ArrayUtils.remove(args, index);
			}
		}
		return args;
	}

	protected static void displayLogo(String version){
		System.out.println(
				" _____       _________  ______   ______    \n" +
				"|_   _|     |  _   _  ||_   _ `.|_   _ \\   \n" +
				"  | |   .--.|_/ | | \\_|  | | `. \\ | |_) |  \n" +
				"  | | / .'`\\ \\  | |      | |  | | |  __'.  \n" +
				" _| |_| \\__. | _| |_    _| |_.' /_| |__) | \n" +
				"|_____|'.__.' |_____|  |______.'|_______/  version "+version+"\n" +
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
				System.out.println(((TsfileDatabaseMetadata)connection.getMetaData()).getMetadataInJson());
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
