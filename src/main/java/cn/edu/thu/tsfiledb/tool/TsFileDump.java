package cn.edu.thu.tsfiledb.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;

/**
 * Tsfile dump to CSV Class
 *
 * @author chengaru
 */
public class TsFileDump {

    private static final Logger LOGGER = LoggerFactory.getLogger(TsFileDump.class);

    private static final String HOST_ARGS = "h";
    private static final String HOST_NAME = "host";

    private static final String HELP_ARGS = "help";

    private static final String PORT_ARGS = "p";
    private static final String PORT_NAME = "port";

    private static final String PASSWORD_ARGS = "pw";
    private static final String PASSWORD_NAME = "password";

    private static final String USERNAME_ARGS = "u";
    private static final String USERNAME_NAME = "username";

    private static final String TARGETFILE_ARGS = "tf";
    private static final String TARGETFILE_NAME = "targetfile";

    private static final String TIMEFORMAT_ARGS = "t";
    private static final String TIMEFORMAT_NAME = "timeformat";

    private static final String SQLFILE_ARGS = "s";
    private static final String SQLFILE_NAME = "sqlfile";

    private static final String HEADERDIS_ARGS = "hd";
    private static final String HEADERDIS_NAME = "headerdis";

    private static final String QUERYSQL_ARGS = "qs";
    private static final String QUERYSQL_NAME = "querysql";


    private static final int MAX_HELP_CONSOLE_WIDTH = 88;
    private static final String TSFILEDB_CLI_PREFIX = "Tsfile_Dump";

    private static String host;
    private static String port;
    private static String username;
    private static String password;
    private static String targetfile;
    private static String sqlfile;
    private static String headerdis;
    private static String timeformat;


    private static Connection connection = null;
    private static Statement statement = null;

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        Options options = createOptions();
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
        CommandLine commandLine = null;
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
            System.out.println(String.format(e.getMessage()));
            return;
        }

        try {
            host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine);
            port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine);
            username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine);
            password = commandLine.getOptionValue(PASSWORD_ARGS);
            if (password == null) {
                System.out.print(TSFILEDB_CLI_PREFIX + "> please input password: ");
                password = scanner.nextLine();
            }
            targetfile = checkRequiredArg(TARGETFILE_ARGS, TARGETFILE_NAME, commandLine);

            sqlfile = checkRequiredArg(SQLFILE_ARGS, SQLFILE_NAME, commandLine);
            headerdis = commandLine.getOptionValue(HEADERDIS_ARGS);
            if (headerdis == null) {
                headerdis = "true";
            }
            timeformat = commandLine.getOptionValue(TIMEFORMAT_ARGS);
            if (timeformat == null) {
                timeformat = "ISO8601";
            }
        } catch (ArgsErrorException e) {
            System.out.println("parameters exist error");
            return;
        }

        Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
        connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);

        tsfileDump();
        connection.close();
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

        Option op_host = OptionBuilder.withArgName(HOST_NAME).hasArg().withDescription("Host Name (required)")
                .create(HOST_ARGS);
        options.addOption(op_host);

        Option os_port = OptionBuilder.withArgName(PORT_NAME).hasArg().withDescription("Port (required)")
                .create(PORT_ARGS);
        options.addOption(os_port);

        Option os_username = OptionBuilder.withArgName(USERNAME_NAME).hasArg().withDescription("User name (required)")
                .create(USERNAME_ARGS);
        options.addOption(os_username);

        Option os_password = OptionBuilder.withArgName(PASSWORD_NAME).hasArg().withDescription("Password (required)")
                .create(PASSWORD_ARGS);
        options.addOption(os_password);

        Option os_targetfile = OptionBuilder.withArgName(TARGETFILE_NAME).hasArg().withDescription("TargetFile Path (required)")
                .create(TARGETFILE_ARGS);
        options.addOption(os_targetfile);

        Option os_sqlfile = OptionBuilder.withArgName(SQLFILE_NAME).hasArg().withDescription("Sql Operation(required)")
                .create(SQLFILE_ARGS);
        options.addOption(os_sqlfile);

        Option os_timeformat = OptionBuilder.withArgName(TIMEFORMAT_NAME).hasArg().withDescription("timeFormat  (not required)")
                .create(TIMEFORMAT_ARGS);
        options.addOption(os_timeformat);

        Option os_headerdis = OptionBuilder.withArgName(HEADERDIS_NAME).hasArg().withDescription("headerdis  (not required)")
                .create(HEADERDIS_ARGS);
        options.addOption(os_headerdis);

        return options;
    }

    /**
     * @param arg
     * @param name
     * @param commandLine
     * @return
     * @throws ArgsErrorException
     */
    private static String checkRequiredArg(String arg, String name, CommandLine commandLine) throws ArgsErrorException {
        String str;
        try {
            str = commandLine.getOptionValue(arg);
            return str;
        } catch (Exception e) {
            String msg = String.format("%s: Required values for option '%s' not provided", TSFILEDB_CLI_PREFIX, name);
            System.out.println(msg);
            // System.out.println("Use -help for more information");
        }
        return null;
    }

    public static void tsfileDump() throws SQLException, IOException {
        File f = new File(targetfile);
        if (!f.exists()) {
            f.createNewFile();
        }
        OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(f));
        BufferedWriter writer = new BufferedWriter(write);
        connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
        Statement stmtement = connection.createStatement();

        //TODO
//        if (!sqlfile.startsWith("\"") || !sqlfile.endsWith("\"")) {
//            System.out.println("sql operation must start and end with \" ");
//            return;
//        }

        ResultSet rs = null;
        try {
            rs = stmtement.executeQuery(sqlfile);//obtain query resultset
        }catch (SQLException e) {
            System.out.println(e.getMessage());
            return;
        }

        ResultSetMetaData metadata = rs.getMetaData(); //obtain columnlabel of table

        int count = metadata.getColumnCount(); //obtain columnlcount of table

        if (headerdis.equals("true")) {     //write columnlabel in table
            for (int i = 0; i < count; i++) {
                if (i < count - 1) {
                    writer.write(metadata.getColumnLabel(i) + ",");
                } else
                    writer.write(metadata.getColumnLabel(i) + "\n");
            }
        }
        while (rs.next()) {                //write querydata in table
            if (rs.getString(0) == "null") {
                writer.write(",");
            } else {
                if (timeformat.equals("ISO8601")) {
                    SimpleDateFormat tformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
                    writer.write(tformat.format(rs.getDate(0)) + ",");
                } else if (timeformat.equals("timestamp")) {
                    writer.write(rs.getLong(0) + ",");
                } else {
                    SimpleDateFormat tformat = new SimpleDateFormat(timeformat);
                    writer.write(tformat.format(rs.getDate(0)) + ",");
                }
                for (int j = 1; j < count; j++) {
                    if (j < count - 1) {
                        if (rs.getString(j) == "null") {
                            writer.write(",");
                        } else {
                            writer.write(rs.getString(j) + ",");
                        }
                    } else {
                        if (rs.getString(j) == "null") {
                            writer.write("\n");
                        } else {
                            writer.write(rs.getString(j) + "\n");
                        }
                    }
                }

            }
        }
        System.out.println("file dump successful!");
        writer.flush();
        write.close();
        writer.close();
    }
}


