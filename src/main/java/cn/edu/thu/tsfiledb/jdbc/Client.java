package cn.edu.thu.tsfiledb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import jline.console.ConsoleReader;
import jline.console.completer.Completer;

public class Client extends AbstractClient {

	private static final String[] args = new String[]{
			"show", "set", "select", "drop", "update", "delete", "create", "insert",
			"timeseries", "time_display_type", "time_zone","storage", "group", "time", "timestamp", "values", "now()", "index", "fetch_size",
			"count", "max_time", "min_time", "max_value", "min_value",
			"from", "where", "to", "on",
			"and", "or",
			"user", "role",
			"exit", "quit"};

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		Options options = createOptions();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
		CommandLine commandLine = null;
		CommandLineParser parser = new DefaultParser();

		if (args == null || args.length == 0) {
			System.out.println("Require more params input, please check the following hint.");
			hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
			return;
		}
		init();
		args = checkPasswordArgs(args);
		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption(HELP_ARGS)) {
				hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
				return;
			}
			if (commandLine.hasOption(ISO8601_ARGS)) {
				setTimeFormat("long");
			}
			if (commandLine.hasOption(MAX_PRINT_ROW_COUNT_ARGS)) {
				try {
					maxPrintRowCount = Integer.valueOf(commandLine.getOptionValue(MAX_PRINT_ROW_COUNT_ARGS));
					if (maxPrintRowCount < 0) {
						maxPrintRowCount = Integer.MAX_VALUE;
					}
				} catch (NumberFormatException e) {
					System.out.println(
							TSFILEDB_CLI_PREFIX + "> error format of max print row count, it should be number");
					return;
				}
			}
		} catch (ParseException e) {
			System.out.println("Require more params input, please check the following hint.");
			hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
			return;
		}

		ConsoleReader reader = null;
		try {
			reader = new ConsoleReader();
			reader.setExpandEvents(false);
			reader.addCompleter(new MyCompleter());
			String s;
			try {
				String host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine);
				String port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine);
				String username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine);

				String password = commandLine.getOptionValue(PASSWORD_ARGS);
				if (password == null) {
					password = reader.readLine("please input your password:", '\0');
				}
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username,
							password);
				} catch (SQLException e) {
					System.out.println(TSFILEDB_CLI_PREFIX + "> " + e.getMessage());
					return;
				}
			} catch (ArgsErrorException e) {
//				System.out.println(TSFILEDB_CLI_PREFIX + ": " + e.getMessage());
				return;
			}

			displayLogo();
			System.out.println(TSFILEDB_CLI_PREFIX + "> login successfully");
			
			while (true) {
				s = reader.readLine(TSFILEDB_CLI_PREFIX + "> ", null);
				if (s == null) {
					continue;
				} else {
					String[] cmds = s.trim().split(";");
					for (int i = 0; i < cmds.length; i++) {
						String cmd = cmds[i];
						if (cmd != null && !cmd.trim().equals("")) {
							OPERATION_RESULT result = handleInputInputCmd(cmd, connection);
							switch (result) {
							case RETURN_OPER:
								return;
							case CONTINUE_OPER:
								continue;
							default:
								break;
							}
						}
					}
				}
			}
		} catch (Exception e) {
			System.out.println(TSFILEDB_CLI_PREFIX + "> exit client with error " + e.getMessage());
		} finally {
			if (reader != null) {
				reader.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}

	static class MyCompleter implements Completer{

		@Override
		public int complete(String buffer, int cursor, List<CharSequence> candidates) {
			if (buffer.length() > 0 && cursor > 0) {
				int lastIndex = buffer.lastIndexOf(' ');
				if(lastIndex > cursor) return cursor;
				String substring = buffer.substring(lastIndex+1, cursor);
				if(substring == null || substring.trim().equals("")) return cursor;
				for(String arg : args){
					if (arg.startsWith(substring)) {
						candidates.add(arg);	
					}
				}
				return lastIndex+1;
			}
			return cursor;
		}
	}
}
