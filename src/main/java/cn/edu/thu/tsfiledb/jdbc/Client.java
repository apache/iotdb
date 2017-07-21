package cn.edu.thu.tsfiledb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import jline.console.ConsoleReader;

public class Client extends AbstractClient {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		Options options = createOptions();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
		CommandLine commandLine = null;
		CommandLineParser parser = new DefaultParser();

		if (args == null || args.length == 0) {
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
			hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
			return;
		}

		ConsoleReader reader = null;
		try {
			String s;
			try {
				String host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine);
				String port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine);
				String username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine);

				String password = commandLine.getOptionValue(PASSWORD_ARGS);
				if (password == null) {
					password = readPassword();
				}
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username,
							password);
				} catch (SQLException e) {
					System.out.println(TSFILEDB_CLI_PREFIX + "> " + e.getMessage());
					return;
				}
			} catch (ArgsErrorException e) {
				System.out.println(TSFILEDB_CLI_PREFIX + ": " + e.getMessage());
				return;
			}

			displayLogo();
			System.out.println(TSFILEDB_CLI_PREFIX + "> login successfully");
			
			reader = new ConsoleReader();
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

}
