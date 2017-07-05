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

public class Client {

	private static final int MAX_PRINT_ROW_COUNT = 1000;
	private static final int MAX_HELP_CONSOLE_WIDTH = 66;
	
	private static final String QUIT_COMMAND = "quit";
	private static final String SHOW_METADATA_COMMAND = "show metadata";

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		boolean printToConsole = true;

		Options options = createOptions();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
		CommandLine commandLine = null;
		CommandLineParser parser = new DefaultParser();
		
		if(args == null || args.length == 0){
			hf.printHelp("TsFileDB Client-Cli", options, true);
			return;
		}
		
		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption("help")) {
				hf.printHelp("TsFileDB Client-Cli", options, true);
				return;
			}
		} catch (ParseException e) {
			hf.printHelp("TsFileDB Client-Cli", options, true);
			return;
		}	

		try {
			String host = checkRequiredArg("h", "host", commandLine);
			String port = checkRequiredArg("p", "port", commandLine);
			String username = checkRequiredArg("u", "username", commandLine);
			String password = checkRequiredArg("pw", "password", commandLine);
			try{
				connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
			} catch (SQLException e) {
				System.out.println("TsFileDB Client-Cli: "+e.getMessage());
				return;
			}
		} catch (ArgsErrorException e1) {
			return;
		}

		System.out.println(" _______________________________.___.__          \n"
				+ " \\__    ___/   _____/\\_   _____/|   |  |   ____  \n"
				+ "   |    |  \\_____  \\  |    __)  |   |  | _/ __ \\ \n"
				+ "   |    |  /        \\ |     \\   |   |  |_\\  ___/ \n"
				+ "   |____| /_______  / \\___  /   |___|____/\\___  >   version 0.0.1\n"
				+ "                  \\/      \\/                  \\/ \n");

		System.out.println("TsFileDB Client-Cli: login successfully");
		Scanner scanner = null;
		try {
			String s;
			scanner = new Scanner(System.in);
			while (true) {
				System.out.print("[TsFileDB Client-Cli: ] > ");
				s = scanner.nextLine();
				String[] cmds = s.trim().split(";");
				for (int i = 0; i < cmds.length; i++) {
					String cmd = cmds[i];
					if (cmd != null && !cmd.trim().equals("")) {
						if(cmd.toLowerCase().equals(QUIT_COMMAND)){
							System.out.println(">> TsFileDB Client-Cli Quit");
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
								output(resultSet, printToConsole);
							}
							statement.close();
							System.out.println("TsFileDB Client-Cli: execute successfully.");
						} catch (TsfileSQLException e) {
							System.out.println("TsFileDB Client-Cli: statement error: " + e.getMessage());
						} catch (Exception e) {
							System.out.println("TsFileDB Client-Cli: connection error. " + e.getMessage());
						}
					}
				}
			}
			
		} catch (Exception e) {
			System.out.println("TsFileDB Client-Cli: exit client with error "+e.getMessage());
		} finally {
			if(scanner != null){
				scanner.close();
			}
			if(connection != null){
				connection.close();
			}
			
		}
	}

	public static void output(ResultSet res, boolean printToConsole) {
		try {
			int cnt = 0;
			int colCount = res.getMetaData().getColumnCount();
			// //Output Labels

			StringBuilder blockLine = new StringBuilder();
			int maxTimeLength = 30;
			int maxValueLength = 15;
			String formatTime = "|%"+maxTimeLength+"s|";
			String formatValue = "|%"+maxValueLength+"s|";
			if (printToConsole) {
				
				for (int i = 0; i < colCount; i++) {
					int len = res.getMetaData().getColumnLabel(i).length();
					maxValueLength = maxValueLength < len ? len : maxValueLength;
				}
				blockLine.append("+").append(StringUtils.repeat('-', maxTimeLength)).append("+");
				for (int i = 0; i < colCount - 1; i++) {
				    blockLine.append(StringUtils.repeat('-', maxValueLength)).append("+");
				}
				System.out.println(blockLine);

				formatValue = "%" + maxValueLength + "s|";
				System.out.printf(formatTime, "Timestamp");
				for (int i = 0; i < colCount - 1; i++) {
					System.out.printf(formatValue, res.getMetaData().getColumnLabel(i + 1));
				}
				System.out.printf("\n");

				System.out.println(blockLine);
			}

			// Output values
			while (res.next()) {

				if (printToConsole && cnt < MAX_PRINT_ROW_COUNT) {
					System.out.printf(formatTime, formatDatetime(res.getLong(0)));
				}

				for (int i = 1; i < colCount; i++) {
					if (printToConsole && cnt < MAX_PRINT_ROW_COUNT) {
						System.out.printf(formatValue, String.valueOf(res.getString(i)));
					}
				}

				if (printToConsole && cnt < MAX_PRINT_ROW_COUNT) {
					System.out.printf("\n");
				}
				cnt++;

				if (!printToConsole && cnt % 10000 == 0) {
					System.out.println(cnt);
				}
			}

			if (printToConsole) {
			    System.out.println(blockLine);
			}

			System.out.println("Result size : " + cnt);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private static Options createOptions() {
		Options options = new Options();
		Option help = new Option("help", false, "Display help information");
		help.setRequired(false);
		options.addOption(help);

		Option host = OptionBuilder.withArgName("host").hasArg().withDescription("Host Name (required)").create("h");
		options.addOption(host);

		Option port = OptionBuilder.withArgName("port").hasArg().withDescription("Port (required)").create("p");
		options.addOption(port);

		Option username = OptionBuilder.withArgName("username").hasArg().withDescription("User name (required)").create("u");
		options.addOption(username);

		Option password = OptionBuilder.withArgName("password").hasArg().withDescription("Password (required)").create("pw");
		options.addOption(password);

		return options;
	}
	
	private static String formatDatetime(long timestamp){
	    DateTime dateTime = new DateTime(timestamp);
	    return dateTime.toString();
	}
	
	private static String checkRequiredArg(String arg,String name, CommandLine commandLine) throws ArgsErrorException{
		String str = commandLine.getOptionValue(arg);
		if(str == null){
			String msg = String.format("TsFileDB Client-Cli: Required values for option '%s' not provided",name);
			System.out.println(msg);
			System.out.println("Use -help for more information");
			throw new ArgsErrorException(msg);
		}
		return str;
	}
}
