package cn.edu.thu.tsfiledb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import jline.console.ConsoleReader;

public class WinClient {
	private static final String HOST_ARGS = "h";
	private static final String HOST_NAME = "host";
	
	private static final String HELP_ARGS = "help";
	
	private static final String PORT_ARGS = "p";
	private static final String PORT_NAME = "port";
	
	private static final String PASSWORD_ARGS = "pw";
	private static final String PASSWORD_NAME = "password";
	
	private static final String USERNAME_ARGS = "u";
	private static final String USERNAME_NAME = "username";
	
	private static final String ISO8601_ARGS = "disableISO8601";
	private static boolean timeFormatInISO8601 = true;
	
	private static final String MAX_PRINT_ROW_COUNT_ARGS = "maxPRC";
	private static final String MAX_PRINT_ROW_COUNT_NAME = "maxPrintRowCount";
	private static int maxPrintRowCount = 1000;
	
	private static final String TSFILEDB_CLI_PREFIX = "TsFileDB";
	private static final String QUIT_COMMAND = "quit";
	private static final String EXIT_COMMAND = "exit";
	private static final String SHOW_METADATA_COMMAND = "show timeseries";
	private static final int MAX_HELP_CONSOLE_WIDTH = 88;
	
	private static final String[] AGGREGATION_OPERATOR = new String[]{"count", "min_timestamp", "max_timestamp", "max_value", "min_value"};
	
	private static final String TIMESTAMP_STR = "Timestamp";

	private static int maxTimeLength = 30;
	private static int maxValueLength = 15;
	private static String formatTime = "%"+maxTimeLength+"s|";
	private static String formatValue = "%"+maxValueLength+"s|";
	

	public static void output(ResultSet res, boolean printToConsole, String statement) {
		try {
			int cnt = 0;
			int colCount = res.getMetaData().getColumnCount();
			
			boolean printTimestamp = printTimestamp(statement);
			boolean printHeader = false;
			

			// Output values
			while (res.next()) {	

				// Output Labels
				if(!printHeader){
					printBlockLine(printTimestamp, colCount, res);
					printName(printTimestamp, colCount, res);
					printBlockLine(printTimestamp, colCount, res);
					printHeader = true;
				}
			    	if(cnt < maxPrintRowCount){
			    	    	System.out.print("|");
			    	    	if(printTimestamp){
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
			if(!printHeader){
				printBlockLine(printTimestamp, colCount, res);
				printName(printTimestamp, colCount, res);
				printBlockLine(printTimestamp, colCount, res);
				printHeader = true;
			}
			printBlockLine(printTimestamp, colCount, res);
			System.out.println(TSFILEDB_CLI_PREFIX+"> record number = " + cnt);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private static Options createOptions() {
		Options options = new Options();
		Option help = new Option(HELP_ARGS, false, "Display help information");
		help.setRequired(false);
		options.addOption(help);
		
		Option timeFormat = new Option(ISO8601_ARGS, false, "Display timestamp in number");
		timeFormat.setRequired(false);
		options.addOption(timeFormat);

		Option host = OptionBuilder.withArgName(HOST_NAME).hasArg().withDescription("Host Name (required)").create(HOST_ARGS);
		options.addOption(host);

		Option port = OptionBuilder.withArgName(PORT_NAME).hasArg().withDescription("Port (required)").create(PORT_ARGS);
		options.addOption(port);

		Option username = OptionBuilder.withArgName(USERNAME_NAME).hasArg().withDescription("User name (required)").create(USERNAME_ARGS);
		options.addOption(username);

		Option password = OptionBuilder.withArgName(PASSWORD_NAME).hasArg().withDescription("Password (required)").create(PASSWORD_ARGS);
		options.addOption(password);

		Option maxPrintCount = OptionBuilder.withArgName(MAX_PRINT_ROW_COUNT_NAME).hasArg().withDescription("Maximum number of rows displayed (optional)").create(MAX_PRINT_ROW_COUNT_ARGS);
		options.addOption(maxPrintCount);
		return options;
	}
	
	private static String formatDatetime(long timestamp){
	    if(timeFormatInISO8601){
		    DateTime dateTime = new DateTime(timestamp);
		    return dateTime.toString();
	    }
	    return timestamp+"";
	}
	
	private static String checkRequiredArg(String arg,String name, CommandLine commandLine) throws ArgsErrorException{
		String str = commandLine.getOptionValue(arg);
		if(str == null){
			String msg = String.format("%s: Required values for option '%s' not provided", TSFILEDB_CLI_PREFIX, name);
			System.out.println(msg);
			System.out.println("Use -help for more information");
			throw new ArgsErrorException(msg);
		}
		return str;
	}
	
	private static void setTimeFormatForNumber(){
	    timeFormatInISO8601 = false;
	    maxTimeLength = maxValueLength;
	    formatTime = "%"+maxTimeLength+"s|";
	}
	
	private static void printBlockLine(boolean printTimestamp, int colCount, ResultSet res) throws SQLException{
                StringBuilder blockLine = new StringBuilder();
                for (int i = 0; i < colCount - 1; i++) {
			int len = res.getMetaData().getColumnLabel(i+1).length();
			maxValueLength = maxValueLength < len ? len : maxValueLength;
		}
		if(printTimestamp){
		    	blockLine.append("+").append(StringUtils.repeat('-', maxTimeLength)).append("+");
		} else{
		    blockLine.append("+");
		}
		for (int i = 0; i < colCount - 1; i++) {
		    blockLine.append(StringUtils.repeat('-', maxValueLength)).append("+");
		}
		System.out.println(blockLine);
	}
	
	private static void printName(boolean printTimestamp, int colCount, ResultSet res) throws SQLException{
	    	System.out.print("|");
		formatValue = "%" + maxValueLength + "s|";
		if(printTimestamp){
			System.out.printf(formatTime, TIMESTAMP_STR);
		}
		for (int i = 0; i < colCount - 1; i++) {
			System.out.printf(formatValue, res.getMetaData().getColumnLabel(i + 1));
		}
		System.out.printf("\n");
	}
	
	private static boolean printTimestamp(String statement){
	    if(statement != null){
		for(String operator : AGGREGATION_OPERATOR){
		    if(statement.indexOf(operator) != -1){
			return false;
		    }
		} 
	    }
	    return true;
	}
	
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		boolean printToConsole = true;
		Options options = createOptions();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
		CommandLine commandLine = null;
		CommandLineParser parser = new DefaultParser();
		Scanner scanner = new Scanner(System.in);
		
		if(args == null || args.length == 0){
			hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
			return;
		}
		
		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption(HELP_ARGS)) {
				hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
				return;
			}
			if(commandLine.hasOption(ISO8601_ARGS)){
			    setTimeFormatForNumber();
			}
			if(commandLine.hasOption(MAX_PRINT_ROW_COUNT_ARGS)){
			    try {
				maxPrintRowCount = Integer.valueOf(commandLine.getOptionValue(MAX_PRINT_ROW_COUNT_ARGS));
				if(maxPrintRowCount < 0){
				    maxPrintRowCount = Integer.MAX_VALUE;
				}
			    } catch (NumberFormatException e) {
				System.out.println(TSFILEDB_CLI_PREFIX+"> error format of max print row count, it should be number");
				return;
			    }  
			}
		} catch (ParseException e) {
			hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
			return;
		}	
		try {
			String s;
			String osName = System.getProperty("os.name");
			

			try {
				String host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine);
				String port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine);
				String username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine);
				
				String password = commandLine.getOptionValue(PASSWORD_ARGS);
				if(password == null){	
					System.out.print(TSFILEDB_CLI_PREFIX+"> please input password: ");
				    password = scanner.nextLine();
				}
				try{
					connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
				} catch (SQLException e) {
					System.out.println(TSFILEDB_CLI_PREFIX+"> "+e.getMessage());
					return;
				}
			} catch (ArgsErrorException e) {
			    	System.out.println(TSFILEDB_CLI_PREFIX+": "+e.getMessage());
				return;
			}

			
			System.out.println(" _______________________________.___.__          \n"
				+ " \\__    ___/   _____/\\_   _____/|   |  |   ____  \n"
				+ "   |    |  \\_____  \\  |    __)  |   |  | _/ __ \\ \n"
				+ "   |    |  /        \\ |     \\   |   |  |_\\  ___/ \n"
				+ "   |____| /_______  / \\___  /   |___|____/\\___  >   version 0.0.1\n"
				+ "                  \\/      \\/                  \\/ \n");

			System.out.println(TSFILEDB_CLI_PREFIX+"> login successfully");			
			
			while (true) {
					System.out.print(TSFILEDB_CLI_PREFIX+"> ");
			    	s = scanner.nextLine();
//			    	System.out.println("\n");
				if(s == null){
				    continue;
				} else{
					String[] cmds = s.trim().split(";");
					for (int i = 0; i < cmds.length; i++) {
						String cmd = cmds[i];
						if (cmd != null && !cmd.trim().equals("")) {
							if(cmd.toLowerCase().equals(QUIT_COMMAND) || cmd.toLowerCase().equals(EXIT_COMMAND)){
								System.out.println(TSFILEDB_CLI_PREFIX+"> "+cmd.toLowerCase()+ " normally");
								return;
							}
							
							if(cmd.toLowerCase().equals(SHOW_METADATA_COMMAND)){
								System.out.println(connection.getMetaData());
								continue;
							}
							
							Statement statement = connection.createStatement();
							try {
								boolean hasResultSet = statement.execute(cmd.trim());
								if (hasResultSet) {
									ResultSet resultSet = statement.getResultSet();
									output(resultSet, printToConsole, cmd.trim());
								}
								statement.close();
								System.out.println(TSFILEDB_CLI_PREFIX+"> execute successfully.");
							} catch (TsfileSQLException e) {
								System.out.println(TSFILEDB_CLI_PREFIX+"> statement error: " + e.getMessage());
							} catch (Exception e) {
								System.out.println(TSFILEDB_CLI_PREFIX+"> connection error: " + e.getMessage());
							}
						}
					}
				}
			}
		} catch (Exception e) {
			System.out.println(TSFILEDB_CLI_PREFIX+"> exit client with error "+e.getMessage());
		} finally {

		    if(scanner != null){
		    	scanner.close();
		    }
			if(connection != null){
				connection.close();
			}
		}
	}

}
