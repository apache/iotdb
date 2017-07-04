package cn.edu.thu.tsfiledb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
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
import org.joda.time.DateTime;

public class Client {

	private static final int MAX_PRINT_ROW_COUNT = 1000;
	private static final int MAX_HELP_CONSOLE_WIDTH = 66;

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		boolean printToConsole = true;

		try {
			Options options = createOptions();
			HelpFormatter hf = new HelpFormatter();
			hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
			CommandLine commandLine = null;
			CommandLineParser parser = new DefaultParser();
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
			String host = commandLine.getOptionValue("h");
			String port = commandLine.getOptionValue("p");
			String username = commandLine.getOptionValue("u");
			String password = commandLine.getOptionValue("pw");
			
			if(host == null || port == null || username == null || password == null){
				System.out.println("Error input args, input like -h 127.0.0.1 -p 6667 -u myusername -pw mypassword");
				return;
			}
			
			// Login in and create connection first
			try {
				connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
			} catch (SQLTimeoutException e) {
				System.out.println("Connect Timeout: " + e.getMessage());
				return;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				return;
			}
		} catch (Exception e) {
			System.out.println("Unknown Error: " + e.getMessage());
		}

		System.out.println(" _______________________________.___.__          \n"
				+ " \\__    ___/   _____/\\_   _____/|   |  |   ____  \n"
				+ "   |    |  \\_____  \\  |    __)  |   |  | _/ __ \\ \n"
				+ "   |    |  /        \\ |     \\   |   |  |_\\  ___/ \n"
				+ "   |____| /_______  / \\___  /   |___|____/\\___  >   version 0.0.1\n"
				+ "                  \\/      \\/                  \\/ \n");

		System.out.println("login successfully");
		Scanner scanner = null;
		try {
			String s;
			scanner = new Scanner(System.in);
			while (true) {
				System.out.print("[TSFile: ] > ");
				s = scanner.nextLine();
				String[] cmds = s.toLowerCase().trim().split(";");
				for (int i = 0; i < cmds.length; i++) {
					String cmd = cmds[i];
					if (cmd != null && !cmd.trim().equals("")) {
						if(cmd.equals("quit")){
							System.out.println(">> TSFile Quit");
							return;
						}
						
						if(cmd.equals("show metadata")){
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
							System.out.println("Execute successfully.");
						} catch (TsfileSQLException e) {
							System.out.println("statement error: " + e.getMessage());
						} catch (Exception e) {
							System.out.println("Connection error. " + e.getMessage());
						}
					}
				}
			}
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			System.out.println("Exit client with error");
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
			String format = "|%30s|";
			String blockLine = "";
			if (printToConsole) {
				int maxv = 30;
				for (int i = 0; i < colCount; i++) {
					int len = res.getMetaData().getColumnLabel(i).length();
					maxv = maxv < len ? len : maxv;
				}

				for (int i = 0; i < maxv; i++) {
					blockLine += "-";
				}
				System.out.printf("+" + blockLine + "+");
				for (int i = 0; i < colCount - 1; i++) {
					System.out.printf(blockLine + "+");
				}
				System.out.printf("\n");

				format = "%" + maxv + "s|";
				System.out.printf("|" + format, "Timestamp");
				for (int i = 0; i < colCount - 1; i++) {
					System.out.printf(format, res.getMetaData().getColumnLabel(i + 1));
				}
				System.out.printf("\n");

				System.out.printf("+" + blockLine + "+");
				for (int i = 0; i < colCount - 1; i++) {
					System.out.printf(blockLine + "+");
				}
				System.out.printf("\n");
			}

			// Output values
			while (res.next()) {

				if (printToConsole && cnt < MAX_PRINT_ROW_COUNT) {
					System.out.printf("|" + format, formatDatetime(res.getLong(0)));
				}

				for (int i = 1; i < colCount; i++) {
					if (printToConsole && cnt < MAX_PRINT_ROW_COUNT) {
						System.out.printf(format, String.valueOf(res.getString(i)));
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
				System.out.printf("+" + blockLine + "+");
				for (int i = 0; i < colCount - 1; i++) {
					System.out.printf(blockLine + "+");
				}
				System.out.printf("\n");
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

		Option host = OptionBuilder.withArgName("host").hasArg().withDescription("Host Name").create("h");
		options.addOption(host);

		Option port = OptionBuilder.withArgName("port").hasArg().withDescription("Port").create("p");
		options.addOption(port);

		Option username = OptionBuilder.withArgName("username").hasArg().withDescription("User name").create("u");
		options.addOption(username);

		Option password = OptionBuilder.withArgName("password").hasArg().withDescription("Password").create("pw");
		options.addOption(password);

		return options;
	}

	
	private static String formatDatetime(long timestamp){
	    DateTime dateTime = new DateTime(timestamp);
	    return dateTime.toString();
	}
}
