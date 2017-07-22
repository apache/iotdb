package cn.edu.thu.tsfiledb.tool;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import org.apache.commons.cli.*;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


/**
 * CSV File To TsFileDB
 *
 * @author zhanggr
 */
public class ImportCsv {
    private static final String HOST_ARGS = "h";
    private static final String HOST_NAME = "host";

    private static final String HELP_ARGS = "help";

    private static final String PORT_ARGS = "p";
    private static final String PORT_NAME = "port";

    private static final String PASSWORD_ARGS = "pw";
    private static final String PASSWORD_NAME = "password";

    private static final String USERNAME_ARGS = "u";
    private static final String USERNAME_NAME = "username";

    private static final String FILE_ARGS = "f";
    private static final String FILE_NAME = "file";

    private static final String TIME_FORMAT_ARGS = "t";
    private static final String TIME_FORMAT_NAME = "timeformat";

    private static final int MAX_HELP_CONSOLE_WIDTH = 88;
    private static final String TSFILEDB_CLI_PREFIX = "ImportCsv";

    private static final String STRING_DATA_TYPE = "TEXT";
    private static final int BATCH_EXECUTE_COUNT = 10;

    private static String host;
    private static String port;
    private static String username;
    private static String password;
    private static String filename;
    private static String timeFormat = "timestamps";
    private static String errorInsertInfo = "";

    private static HashMap<String, String> timeseriesToType = null;

    private static HashMap<String, ArrayList<Integer>> deviceToColumn;  // storage csv table head info
    private static ArrayList<String> headInfo; // storage csv table head info
    private static ArrayList<String> colInfo; // storage csv device sensor info, corresponding csv table head

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

        Option opFile = Option.builder(FILE_ARGS).longOpt(FILE_NAME).required().argName(FILE_NAME).hasArg().desc("Csv file path (required)").build();
        options.addOption(opFile);

        Option opTimeFormat = Option.builder(TIME_FORMAT_ARGS).optionalArg(true).argName(TIME_FORMAT_NAME).hasArg().desc("Time Format (optional),"
                + " you can choose 1) timestamp 2) ISO8601 3) user-defined pattern like yyyy-MM-dd HH:mm:ss, default timestamp").build();
        options.addOption(opTimeFormat);

        Option opHelp = Option.builder(HELP_ARGS).longOpt(HELP_ARGS).hasArg(false).desc("Display help information").build();
        options.addOption(opHelp);

        return options;
    }

    /**
     * @param tf        time format, optioned:ISO8601, timestamps, self-defined:
     *                  yyyy-mm-dd hh:mm:ss
     * @param timestamp the time column of the csv file
     * @return the timestamps will insert into SQL
     */
    private static String setTimestamp(String tf, String timestamp) {
        if (tf.equals("ISO8601")) {
            return timestamp;
        } else if (tf.equals("timestamps")) {
            try {
                Long.parseLong(timestamp);
            } catch (NumberFormatException nfe) {
                return "";
            }
            return timestamp;
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat(tf);
            Date date = null;
            try {
                date = sdf.parse(timestamp);
            } catch (java.text.ParseException e) {
//				LOGGER.error("input time format error please re-input, Unparseable date: {}", tf, e);
            }
            if (date != null)
                return date.getTime() + "";
            return "";
        }
    }

    /**
     * Data from csv To tsfile
     */
    public static void loadDataFromCSV() {
        Connection connection = null;
        Statement statement = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
        File errorFile = new File(errorInsertInfo);

        try {
            Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
            br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filename))));
            errorFile.createNewFile();
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(errorFile)));
            String line = "";
            String[] strHeadInfo = br.readLine().split(",");
            deviceToColumn = new HashMap<>();
            colInfo = new ArrayList<>();
            headInfo = new ArrayList<>();

            if (strHeadInfo.length <= 1) {
                System.out.println("The CSV file illegal, please check first line");
                br.close();
                connection.close();
                System.exit(1);
            }

            timeseriesToType = new HashMap<>();
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            for (int i = 1; i < strHeadInfo.length; i++) {

                ResultSet resultSet = databaseMetaData.getColumns(null, null, strHeadInfo[i], null);
                if (resultSet.next()) {
                    timeseriesToType.put(resultSet.getString(0), resultSet.getString(1));
                } else {
                    System.out.println("database Cannot find " + strHeadInfo[i] + ", stop import!");
                    bw.write("database Cannot find " + strHeadInfo[i] + ", stop import!");
                    br.close();
                    bw.close();
                    connection.close();
                    System.exit(1);
                }
                headInfo.add(strHeadInfo[i]);
                String deviceInfo = strHeadInfo[i].substring(0, strHeadInfo[i].lastIndexOf("."));

                if (!deviceToColumn.containsKey(deviceInfo)) {
                    deviceToColumn.put(deviceInfo, new ArrayList<>());
                }
                // storage every device's sensor index info
                deviceToColumn.get(deviceInfo).add(i - 1);
                colInfo.add(strHeadInfo[i].substring(strHeadInfo[i].lastIndexOf(".") + 1));
            }

            statement = connection.createStatement();
            int count = 0;
            while ((line = br.readLine()) != null) {
                List<String> sqls = createInsertSQL(line, bw);
                for (String str : sqls) {
                    try {
                        count++;
                        statement.addBatch(str);
                        if (count == BATCH_EXECUTE_COUNT) {
                            statement.executeBatch();
                            statement.clearBatch();
                            count = 0;
                        }
                    } catch (SQLException e) {
//						LOGGER.error("{} :excuted fail!");
                        bw.write(str);
                        bw.newLine();
                    }
                }
            }
            try {
                statement.executeBatch();
                statement.clearBatch();
                System.out.println("excuted finish!");
            } catch (SQLException e) {
                bw.write(e.getMessage());
                bw.newLine();
            }

        } catch (FileNotFoundException e) {
            System.out.println("The csv file " + filename + " can't find!");
        } catch (IOException e) {
            System.out.println("CSV file read exception!" + e.getMessage());
        } catch (ClassNotFoundException e2) {
            System.out.println("Cannot find cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
        } catch (SQLException e) {
            System.out.println("database connection exception!" + e.getMessage());
        } finally {
            try {
                bw.close();
                statement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (errorFile.exists() && (errorFile.length() == 0)) {
            errorFile.delete();
        }
    }

    private static String typeIdentify(String timeseries) {
        return timeseriesToType.get(timeseries);
    }

    /**
     * create Insert SQL statement according to every line csv data
     *
     * @param line          csv line data
     * @param bwToErrorFile the error info insert to file
     * @return ArrayList<String> SQL statement list
     * @throws IOException
     */
    private static List<String> createInsertSQL(String line, BufferedWriter bwToErrorFile) throws IOException {
        String[] data = line.split(",", headInfo.size() + 1);

        List<String> sqls = new ArrayList<>();
        Iterator<Map.Entry<String, ArrayList<Integer>>> it = deviceToColumn.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ArrayList<Integer>> entry = it.next();
            StringBuilder sbd = new StringBuilder();
            ArrayList<Integer> colIndex = entry.getValue();
            sbd.append("insert into " + entry.getKey() + "(timestamp");
            int skipcount = 0;
            for (int j = 0; j < colIndex.size(); ++j) {
                if (data[entry.getValue().get(j) + 1].equals("")) {
                    skipcount++;
                    continue;
                }
                sbd.append(", " + colInfo.get(colIndex.get(j)));
            }
            // define every device null value' number, if the number equal the
            // sensor number, the insert operation stop
            if (skipcount == entry.getValue().size())
                continue;

            String timestampsStr = setTimestamp(timeFormat, data[0]);
            if (timestampsStr.equals("")) {
//				LOGGER.error("Time Format Error! {}", line);
                bwToErrorFile.write(line);
                bwToErrorFile.newLine();
                continue;
            }
            sbd.append(") values(").append(timestampsStr);

            for (int j = 0; j < colIndex.size(); ++j) {
                if (data[entry.getValue().get(j) + 1].equals(""))
                    continue;
                if (typeIdentify(headInfo.get(colIndex.get(j))).equals(STRING_DATA_TYPE)) {
                    sbd.append(", \'" + data[colIndex.get(j) + 1] + "\'");
                } else {
                    sbd.append("," + data[colIndex.get(j) + 1]);
                }
            }
            sbd.append(")");
            sqls.add(sbd.toString());
        }

        return sqls;
    }

    public static void main(String[] args) {
        Options options = createOptions();
        HelpFormatter hf = new HelpFormatter();
        hf.setOptionComparator(null);
        hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
        CommandLine commandLine = null;
        CommandLineParser parser = new DefaultParser();
        Scanner scanner = new Scanner(System.in);

        if (args == null || args.length == 0) {
            System.out.println("Too few params input, please check the following hint.");
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            scanner.close();
            return;
        }

        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            scanner.close();
            System.exit(1);
        }
        if (commandLine.hasOption(HELP_ARGS)) {
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            scanner.close();
            return;
        }
        host = commandLine.getOptionValue(HOST_ARGS);
        port = commandLine.getOptionValue(PORT_ARGS);
        username = commandLine.getOptionValue(USERNAME_ARGS);
        password = commandLine.getOptionValue(PASSWORD_ARGS);
        if (password == null) {
            System.out.print(TSFILEDB_CLI_PREFIX + "> please input password: ");
            password = scanner.nextLine();
        }

        if (host == null || port == null || username == null) {
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            scanner.close();
            return;
        }
        timeFormat = commandLine.getOptionValue(TIME_FORMAT_ARGS);
        if (timeFormat == null) {
            timeFormat = "timestamps";
        }

        if (System.getProperty(SystemConstant.TSFILE_HOME) == null) {
            errorInsertInfo = "src/test/resources/csvInsertError.error";
            if (timeFormat.equals("timestamps")) {
                filename = CsvTestDataGen.defaultLongDataGen();
            } else if (timeFormat.equals("ISO8601")) {
                filename = CsvTestDataGen.isoDataGen();
            } else {
                filename = CsvTestDataGen.userSelfDataGen();
            }
        } else {
            errorInsertInfo = System.getProperty(SystemConstant.TSFILE_HOME) + "/csvInsertError.error";
            filename = commandLine.getOptionValue(FILE_ARGS);
            if (filename == null) {
                hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
                scanner.close();
                return;
            }
        }
        loadDataFromCSV();
    }
}
