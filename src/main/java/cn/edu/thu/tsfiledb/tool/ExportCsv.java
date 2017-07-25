package cn.edu.thu.tsfiledb.tool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Scanner;

/**
 * @author aru cheng
 * @version 1.0.0 20170719
 */
public class ExportCsv {

    private static final String DEFAULT_TIME_FORMAT = "ISO8601";

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
    
    private static String host;
    private static String port;
    private static String username;
    private static String password;

    private static final int MAX_HELP_CONSOLE_WIDTH = 88;
    private static final String TSFILEDB_CLI_PREFIX = "ExportCsv";
    
    private static final String DUMP_FILE_NAME = "dump";
    

    private static String targetDirectory;
    private static String timeFormat;

    public static void main(String[] args) {
        Options options = createOptions();
        HelpFormatter hf = new HelpFormatter();
        hf.setOptionComparator(null);  // avoid reordering
        hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
        CommandLine commandLine;
        CommandLineParser parser = new DefaultParser();
        try (Scanner scanner = new Scanner(System.in)) {
            if (args == null || args.length == 0) {
            		System.out.println("Too few params input, please check the following hint.");
            		hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
                return;
            }
            try {
                commandLine = parser.parse(options, args);
            } catch (ParseException e) {
                System.out.println(e.getMessage());
                hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
                return;
            }
            targetDirectory = commandLine.getOptionValue(TARGET_FILE_ARGS);
            if(targetDirectory == null){
            		System.out.println("Target File Path(-tf) cannot be empty!");
            		return;
            }            
            paraseParams(commandLine, scanner);

            
            String sqlFile = commandLine.getOptionValue(SQL_FILE_ARGS);
            String sql;
            if (sqlFile == null) {
                System.out.print(TSFILEDB_CLI_PREFIX + "> please input query: ");
                sql = scanner.nextLine();
                String[] values = sql.trim().split(";");
                for(int i = 0; i < values.length; i++){
                		dumpResult(values[i], i);
                }                
                return;
            } else{
            		dumpFromSQLFile(sqlFile);
            }   
        } catch (ClassNotFoundException e) {
			System.out.println("Failed to dump data because cannot find TsFile JDBC Driver, please check whether you have imported driver or not");
		} catch (SQLException e) {
			System.out.println(String.format("Encounter an error when dumping data, error is %s", e.getMessage()));
		} catch (IOException e) {
			System.out.println(String.format("Failed to operate on file, because %s", e.getMessage()));
		}
    }

    private static void paraseParams(CommandLine commandLine, Scanner scanner){
        host = commandLine.getOptionValue(HOST_ARGS);
        port = commandLine.getOptionValue(PORT_ARGS);
        username = commandLine.getOptionValue(USERNAME_ARGS);
        password = commandLine.getOptionValue(PASSWORD_ARGS);
        if (password == null) {
            System.out.print(TSFILEDB_CLI_PREFIX + "> please input password: ");
            password = scanner.nextLine();
        }
        timeFormat = commandLine.getOptionValue(TIME_FORMAT_ARGS);
        if (timeFormat == null) {
            timeFormat = DEFAULT_TIME_FORMAT;
        }
        
        if(!targetDirectory.endsWith(File.separator)){
        		targetDirectory += File.separator;
        }    	
    }
    
    /**
     * commandline option create
     *
     * @return object Options
     */
    private static Options createOptions() {
        Options options = new Options();

        Option opHost = Option.builder(HOST_ARGS).longOpt(HOST_NAME).required().argName(HOST_NAME).hasArg().desc("Host Name (required)").build();
        options.addOption(opHost);

        Option opPort = Option.builder(PORT_ARGS).longOpt(PORT_NAME).required().argName(PORT_NAME).hasArg().desc("Port (required)").build();
        options.addOption(opPort);

        Option opUsername = Option.builder(USERNAME_ARGS).longOpt(USERNAME_NAME).required().argName(USERNAME_NAME).hasArg().desc("Username (required)").build();
        options.addOption(opUsername);

        Option opPassword = Option.builder(PASSWORD_ARGS).longOpt(PASSWORD_NAME).optionalArg(true).argName(PASSWORD_NAME).hasArg().desc("Password (optional)").build();
        options.addOption(opPassword);

        Option opTargetFile = Option.builder(TARGET_FILE_ARGS).required().argName(TARGET_FILE_NAME).hasArg().desc("Target File Directory (required)").build();
        options.addOption(opTargetFile);

        Option opSqlFile = Option.builder(SQL_FILE_ARGS).argName(SQL_FILE_NAME).hasArg().desc("SQL File Path (optional)").build();
        options.addOption(opSqlFile);

        Option opTimeFormat = Option.builder(TIME_FORMAT_ARGS).argName(TIME_FORMAT_NAME).hasArg().desc("Time Format. "
        		+ "You can choose 1) timestamp 2) ISO8601 3) user-defined pattern like yyyy-MM-dd HH:mm:ss, default timestamp (optional)").build();
        options.addOption(opTimeFormat);

        Option opHelp = Option.builder(HELP_ARGS).longOpt(HELP_ARGS).hasArg(false).desc("Display help information").build();
        options.addOption(opHelp);

        return options;
    }

    private static void dumpFromSQLFile(String filePath) throws ClassNotFoundException, IOException{
    		BufferedReader reader = new BufferedReader(new FileReader(filePath));
    		String sql = null;
    		int index = 0;
    		while((sql = reader.readLine()) != null){
    			try {
				dumpResult(sql, index);
			} catch (SQLException e) {
				System.out.println(String.format("Cannot dump data for statment %s, because ", sql, e.getMessage()));
			}
    			index++;
    		}
    		reader.close();
    }
    
    /**
     * Dump files from database to CSV file
     *
     * @param sql export the result of executing the sql
     * @param index use to create dump file name
     * @throws ClassNotFoundException if cannot find driver
     * @throws SQLException  if SQL is not valid
     */
    private static void dumpResult(String sql, int index) throws ClassNotFoundException, SQLException{
        BufferedWriter writer = null;
        final String path = targetDirectory+DUMP_FILE_NAME+index+".csv";
        try {
            File tf = new File(path);
            if (!tf.exists()) {
                if (!tf.createNewFile()) {
                    System.out.println("could not create target file for sql statement: " + sql);
                    return;
                }
            }
            writer = new BufferedWriter(new FileWriter(tf));
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return;
        }
        
		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
        Connection connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        ResultSetMetaData metadata = rs.getMetaData();
        long startTime = System.currentTimeMillis();
        try {
            int count = metadata.getColumnCount();
            // write data in csv file
            for (int i = 0; i < count; i++) {
                if (i < count - 1) {
                    writer.write(metadata.getColumnLabel(i) + ",");
                } else {
                    writer.write(metadata.getColumnLabel(i) + "\n");
                }
            }
            while (rs.next()) {
                if (rs.getString(0).equals("null")) {
                    writer.write(",");
                } else {
                    switch (timeFormat) {
                        case DEFAULT_TIME_FORMAT: {
                            DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
                            writer.write(fmt.print(rs.getDate(0).getTime()) + ",");
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
            System.out.println(String.format("Statement %s dump to file %s successfully! It costs %dms",sql, path, (System.currentTimeMillis() - startTime)));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            try {            	
                writer.flush();
                writer.close();               
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
}