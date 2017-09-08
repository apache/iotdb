package cn.edu.thu.tsfiledb.jdbc;

import java.io.Console;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;

public class WinClient extends AbstractClient {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		TsfileConnection connection = null;
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
		Scanner scanner = null;
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
					connection = (TsfileConnection) DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username,
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
			scanner = new Scanner(System.in);
			while (true) {
				System.out.print(TSFILEDB_CLI_PREFIX + "> ");
				s = scanner.nextLine();
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
			if (scanner != null) {
				scanner.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}
	
	private static String readPassword() {
		Console c = System.console();
		if (c == null) { // IN ECLIPSE IDE
			System.out.print(TSFILEDB_CLI_PREFIX + "> please input password: ");
			Scanner scanner = new Scanner(System.in);
			return scanner.nextLine();
		} else { // Outside Eclipse IDE
			return new String(c.readPassword(TSFILEDB_CLI_PREFIX + "> please input password: "));
		}
	}
}
