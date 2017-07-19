package cn.edu.thu.tsfiledb.tool;

import org.apache.commons.cli.*;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Scanner;

/**
 * @author aru cheng
 * @version 1.0.0 20170719
 */
public class TsFileDump {

    private static final String HOST_ARGS = "h";
    private static final String HOST_NAME = "host";

    private static final String HELP_ARGS = "help";

    private static final String PORT_ARGS = "p";
    private static final String PORT_NAME = "port";

    private static final String PASSWORD_ARGS = "pw";
    private static final String PASSWORD_NAME = "password";

    private static final String USERNAME_ARGS = "u";
    private static final String USERNAME_NAME = "username";

    private static final String TARGET_FILE_ARGS = "tf";
    private static final String TARGET_FILE_NAME = "targetfile";

    private static final String TIME_FORMAT_ARGS = "t";
    private static final String TIME_FORMAT_NAME = "timeformat";

    private static final String SQL_FILE_ARGS = "s";
    private static final String SQL_FILE_NAME = "sqlfile";

    private static final String HEADER_DIS_ARGS = "hd";
    private static final String HEADER_DIS_NAME = "headerdis";

    private static final int MAX_HELP_CONSOLE_WIDTH = 88;
    private static final String TSFILEDB_CLI_PREFIX = "Tsfile_Dump";

    private static String targetFile;
    private static String sqlFile;
    private static String headerDis;
    private static String timeFormat;

    private static Connection connection = null;

    /**
     * @param args arguments
     * @throws ClassNotFoundException if JDBC driver not found
     * @throws SQLException           if connection error occurred
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Options options = createOptions();
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
        CommandLine commandLine;
        CommandLineParser parser = new DefaultParser();
        Scanner scanner = new Scanner(System.in);

        if (args == null || args.length == 0) {
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            return;
        }

        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption(HELP_ARGS)) {
                hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
                return;
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            return;
        }

        String host = commandLine.getOptionValue(HOST_ARGS);
        String port = commandLine.getOptionValue(PORT_ARGS);
        String username = commandLine.getOptionValue(USERNAME_ARGS);
        String password = commandLine.getOptionValue(PASSWORD_ARGS);
        if (password == null) {
            System.out.print(TSFILEDB_CLI_PREFIX + "> please input password: ");
            password = scanner.nextLine();
        }
        targetFile = commandLine.getOptionValue(TARGET_FILE_ARGS);
        sqlFile = commandLine.getOptionValue(SQL_FILE_ARGS);
        headerDis = commandLine.getOptionValue(HEADER_DIS_ARGS);
        if (headerDis == null) {
            headerDis = "true";
        }
        timeFormat = commandLine.getOptionValue(TIME_FORMAT_ARGS);
        if (timeFormat == null) {
            timeFormat = "ISO8601";
        }

        try {
            Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return;
        }

        try {
            tsFileDump();
        } catch (SQLException | IOException e) {
            System.out.println(e.getMessage());
        } finally {
            connection.close();
        }

        scanner.close();
    }

    /**
     * commandline option create
     *
     * @return object Options
     */
    private static Options createOptions() {
        Options options = new Options();
        Option help = new Option(HELP_ARGS, false, "Display help information");
        help.setRequired(false);
        options.addOption(help);

        Option opHost = Option.builder(HOST_ARGS).argName(HOST_NAME).hasArg().desc("Host Name (required)").build();
        options.addOption(opHost);

        Option opPort = Option.builder(PORT_ARGS).argName(PORT_NAME).hasArg().desc("Port (required)").build();
        options.addOption(opPort);

        Option opUsername = Option.builder(USERNAME_ARGS).argName(USERNAME_NAME).hasArg().desc("User Name (required)").build();
        options.addOption(opUsername);

        Option opPassword = Option.builder(PASSWORD_ARGS).optionalArg(true).argName(PASSWORD_NAME).hasArg().desc("Password (optional)").build();
        options.addOption(opPassword);

        Option opTargetFile = Option.builder(TARGET_FILE_ARGS).argName(TARGET_FILE_NAME).hasArg().desc("Target File Path (required)").build();
        options.addOption(opTargetFile);

        Option opSqlFile = Option.builder(SQL_FILE_ARGS).argName(SQL_FILE_NAME).hasArg().desc("SqlFile Path (required)").build();
        options.addOption(opSqlFile);

        Option opTimeFormat = Option.builder(TIME_FORMAT_ARGS).optionalArg(true).argName(TIME_FORMAT_NAME).hasArg().desc("Time Format (optional)").build();
        options.addOption(opTimeFormat);

        Option opHeaderDis = Option.builder(HEADER_DIS_ARGS).optionalArg(true).argName(HEADER_DIS_NAME).hasArg().desc("Header Display Format (optional)").build();
        options.addOption(opHeaderDis);

        return options;
    }

    /**
     * Dump files from database to CSV file
     *
     * @throws SQLException if SQL is not valid
     * @throws IOException  if file error occurred
     */
    private static void tsFileDump() throws SQLException, IOException {
        Statement statement = connection.createStatement();

        File sf = new File(sqlFile);
        String str;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(sf)));
            str = reader.readLine();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return;
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        ResultSet rs = statement.executeQuery(str);
        BufferedWriter writer;
        ResultSetMetaData metadata = rs.getMetaData();
        try {
            File tf = new File(targetFile);
            if (!tf.exists()) {
                if (!tf.createNewFile()) {
                    System.out.println("could not create target file");
                }
            }
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tf)));
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return;
        }

        try {
            int count = metadata.getColumnCount();
            // write data in csv file
            if (headerDis.equals("true")) {
                for (int i = 0; i < count; i++) {
                    if (i < count - 1) {
                        writer.write(metadata.getColumnLabel(i) + ",");
                    } else
                        writer.write(metadata.getColumnLabel(i) + "\n");
                }
            }
            while (rs.next()) {
                if (rs.getString(0).equals("null")) {
                    writer.write(",");
                } else {
                    switch (timeFormat) {
                        case "ISO8601": {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
                            writer.write(sdf.format(rs.getDate(0)) + ",");
                            break;
                        }
                        case "timestamp":
                            writer.write(rs.getLong(0) + ",");
                            break;
                        default: {
                            SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
                            writer.write(sdf.format(rs.getDate(0)) + ",");
                            break;
                        }
                    }

                    for (int j = 1; j < count; j++) {
                        if (j < count - 1) {
                            if (rs.getString(j).equals("null")) {
                                writer.write(",");
                            } else {
                                writer.write(rs.getString(j) + ",");
                            }
                        } else {
                            if (rs.getString(j).equals("null")) {
                                writer.write("\n");
                            } else {
                                writer.write(rs.getString(j) + "\n");
                            }
                        }
                    }
                }
            }
            System.out.println("file dump successful!");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }
}