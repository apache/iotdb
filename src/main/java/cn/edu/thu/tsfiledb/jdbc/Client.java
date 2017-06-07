package cn.edu.thu.tsfiledb.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;

import jline.console.ConsoleReader;

public class Client {
	
	private static final int MAX_PRINT_ROW_COUNT = 1000;
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		ConsoleReader reader = null;
		String s;
		boolean printToConsole = true;

		try {
			reader = new ConsoleReader();
			String[] argsName = new String[] { "-host", "-port", "-u", "-print" };
			String[] argv = new String[argsName.length];

			if (args.length < 3) {
				System.out.println("Usage : login -host<host> -port<port> -u<username>");
				System.out.println("e.g. : login -host127.0.0.1 -port6667 -u<UserA>");
				return;
			}

			for (int i = 0; i < args.length; i++) {
				String v = args[i].trim();
				for (int j = 0; j < argsName.length; j++) {
					if (v.startsWith(argsName[j])) {
						argv[j] = v.substring(argsName[j].length()).trim();
					}
				}
			}

			if (argv[3] != null && !argv[3].equals("")) {
				if (argv[3].toLowerCase().equals("true") || argv[3].toLowerCase().equals("false")) {
					printToConsole = Boolean.valueOf(argv[3]);
				} else {
					System.out.println("-print args error.");
					return;
				}
			}
			String password = reader.readLine("Password: ", ' ');
			// Login in and create connection first
			try {
				connection = DriverManager.getConnection("jdbc:tsfile://" + argv[0] + ":" + argv[1] + "/x", argv[2],
						password);
			} catch (SQLTimeoutException e){
				System.out.println("Connect Timeout: " + e.getMessage());
				return;
			} catch (SQLException e) {				
				System.out.println(e.getMessage());
				return;
			}

		} catch (IOException e) {
			System.out.println("Console Input Error:" + e.getMessage());
		} catch (Exception e){
			System.out.println("Unknown Error: " + e.getMessage());
		}

		System.out.println(" _______________________________.___.__          \n"
				+ " \\__    ___/   _____/\\_   _____/|   |  |   ____  \n"
				+ "   |    |  \\_____  \\  |    __)  |   |  | _/ __ \\ \n"
				+ "   |    |  /        \\ |     \\   |   |  |_\\  ___/ \n"
				+ "   |____| /_______  / \\___  /   |___|____/\\___  >   version 0.0.1\n"
				+ "                  \\/      \\/                  \\/ \n");
		

		System.out.println("login successfully");

		try {
			while (true) {
				s = reader.readLine("[TSFile: ] > ");
				if (s.toLowerCase().trim().equals("quit")) {
					System.out.println(">> TSFile Quit");
					break;
				}
				if (s.toLowerCase().trim().equals("show metadata")){
					System.out.println(connection.getMetaData());
					continue;
				}
				String[] cmds = s.split(";");
				for (int i = 0; i < cmds.length; i++) {
					String cmd = cmds[i];
					if (cmd != null && !cmd.trim().equals("")) {
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
						} catch (Exception e){
							System.out.println("Connection error. " + e.getMessage());
						}
						
					}
				}
			}
			connection.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			System.out.println("Exit client with error");
		}
	}

	public static void output(ResultSet res, boolean printToConsole) {
		try {
			int cnt = 0;
			int colCount = res.getMetaData().getColumnCount();
			// //Output Labels
			String format = "|%15s|";
			String blockLine = "";
			if (printToConsole) {
				int maxv = 15;
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
				StringBuilder line = new StringBuilder();
				line.append(String.valueOf(res.getString(0)));

				if (printToConsole && cnt < MAX_PRINT_ROW_COUNT) {
					System.out.printf("|" + format, String.valueOf(res.getString(0)));
				}

				for (int i = 1; i < colCount; i++) {
					line.append(",");
					line.append(res.getString(i));
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
}
