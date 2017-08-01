package cn.edu.thu.tsfiledb.tool;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.jdbc.TsfileConnection;
import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * CSV File To TsFileDB
 *
 * @author zhanggr
 */
public class ImportCsv extends AbstractCsvTool{
    private static final String FILE_ARGS = "f";
    private static final String FILE_NAME = "file or folder";
    private static final String FILE_SUFFIX = "csv";

    private static final String TSFILEDB_CLI_PREFIX = "ImportCsv";

    private static final String STRING_DATA_TYPE = "TEXT";
    private static final int BATCH_EXECUTE_COUNT = 10;

    private static String filename;
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

        Option opFile = Option.builder(FILE_ARGS).required().argName(FILE_NAME).hasArg().desc("If input a file path, load a csv file, otherwise load all csv file under this directory (required)").build();
        options.addOption(opFile);

        Option opTimeFormat = Option.builder(TIME_FORMAT_ARGS).optionalArg(true).argName(TIME_FORMAT_NAME).hasArg().desc("Time Format (optional),"
                + " you can choose 1) timestamp 2) ISO8601 3) user-defined pattern like yyyy-MM-dd\\ HH:mm:ss, default timestamp").build();
        options.addOption(opTimeFormat);

        Option opHelp = Option.builder(HELP_ARGS).longOpt(HELP_ARGS).hasArg(false).desc("Display help information").build();
        options.addOption(opHelp);

        return options;
    }

    /**
     * Data from csv To tsfile
     */
    private static void loadDataFromCSV(File file) {
        Statement statement = null;
        BufferedReader br = null;
//        BufferedWriter bw = null;
//        File errorFile = new File(errorInsertInfo+index);
//        boolean errorFlag = true;
        try {
            br = new BufferedReader(new FileReader(file));
//            if(!errorFile.exists()) errorFile.createNewFile();
//            bw = new BufferedWriter(new FileWriter(errorFile));
            String line = "";
            String header = br.readLine();
            String[] strHeadInfo = header.split(",");
            
//            bw.write(header);
//            bw.newLine();
            
            deviceToColumn = new HashMap<>();
            colInfo = new ArrayList<>();
            headInfo = new ArrayList<>();

            if (strHeadInfo.length <= 1) {
                System.out.println("The CSV file" + filename + " illegal, please check first line");
                return;
            }
            long startTime = System.currentTimeMillis();
            timeseriesToType = new HashMap<>();
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            for (int i = 1; i < strHeadInfo.length; i++) {
                ResultSet resultSet = databaseMetaData.getColumns(null, null, strHeadInfo[i], null);
                if (resultSet.next()) {
                    timeseriesToType.put(resultSet.getString(0), resultSet.getString(1));
                } else {
                    System.out.println("database Cannot find " + strHeadInfo[i] + ", stop import!");
//                    bw.write("database Cannot find " + strHeadInfo[i] + ", stop import!");
//                    errorFlag = false;
                    return;
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
                List<String> sqls = createInsertSQL(line);
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
//                        bw.write(e.getMessage());
//                        bw.newLine();
//                        errorFlag = false;
                    }
                }
            }
            try {
                statement.executeBatch();
                statement.clearBatch();
                System.out.println(String.format("load data from %s successfully, it cost %dms", file.getName(), (System.currentTimeMillis()-startTime)));
            } catch (SQLException e) {
//                bw.write(e.getMessage());
//                bw.newLine();
//                errorFlag = false;
            }

        } catch (FileNotFoundException e) {
            System.out.println("The csv file " + filename + " can't find!");
        } catch (IOException e) {
            System.out.println("CSV file read exception!" + e.getMessage());
        } catch (SQLException e) {
            System.out.println("Database connection exception!" + e.getMessage());
        } finally {
            try {
            		if(br != null) br.close();
//            		if(bw != null) bw.close();
            		if(statement != null) statement.close();
//            		if(errorFlag) FileUtils.forceDelete(errorFile);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        if (errorFile.exists() && (errorFile.length() == 0)) {
//            errorFile.delete();
//        }
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
    private static List<String> createInsertSQL(String line) throws IOException {
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

            String timestampsStr = data[0];
            if (timestampsStr.equals("")) {
//                bwToErrorFile.write(line);
//                bwToErrorFile.newLine();
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

    public static void main(String[] args) throws IOException, SQLException {
        Options options = createOptions();
        HelpFormatter hf = new HelpFormatter();
        hf.setOptionComparator(null);
        hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
        CommandLine commandLine = null;
        CommandLineParser parser = new DefaultParser();

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
        if (commandLine.hasOption(HELP_ARGS)) {
            hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
            return;
        }
        
        ConsoleReader reader = new ConsoleReader();
		reader.setExpandEvents(false);
		try {
			parseBasicParams(commandLine, reader);
			Class.forName(JDBC_DRIVER);
			connection = (TsfileConnection) DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
			setTimeZone();
			
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
	                return;
	            }
	        }
	        
	        File file = new File(filename);
	        if(file.isFile() && file.getName().endsWith(FILE_SUFFIX)){
	        		loadDataFromCSV(file);
	        } else{
	        		for(File f : file.listFiles()){
	        			if(f.isFile() && f.getName().endsWith(FILE_SUFFIX)){
	        				loadDataFromCSV(f);
	        			}
	        		}
	        }
			
		} catch (ArgsErrorException e) { 
			
		} catch (ClassNotFoundException e) {
			System.out.println("Failed to dump data because cannot find TsFile JDBC Driver, please check whether you have imported driver or not");
		} catch (TException e) {
			System.out.println(String.format("Encounter an error when connecting to server, because %s", e.getMessage()));
		} finally {
			if(reader != null){
				reader.close();
			}
			if (connection != null){
				connection.close();
			}
		}
			

    }
}
