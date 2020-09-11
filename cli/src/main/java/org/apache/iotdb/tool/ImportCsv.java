/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import jline.console.ConsoleReader;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.thrift.TException;

/**
 * read a CSV formatted data File and insert all the data into IoTDB.
 */
public class ImportCsv extends AbstractCsvTool {
  private static final String FILE_ARGS = "f";
  private static final String FILE_NAME = "file or folder";
  private static final String FILE_SUFFIX = "csv";

  private static final String TSFILEDB_CLI_PREFIX = "ImportCsv";
  private static final String ERROR_INFO_STR = "csvInsertError.error";

  private static final String STRING_DATA_TYPE = "TEXT";
  private static final int BATCH_EXECUTE_COUNT = 100;

  private static String errorInsertInfo = "";
  private static boolean errorFlag;

  private static String IOTDB_CLI_HOME = "IOTDB_CLI_HOME";

  private static int count;
  private static Statement statement;

  /**
   * create the commandline options.
   *
   * @return object Options
   */
  private static Options createOptions() {
    Options options = new Options();

    Option opHost = Option.builder(HOST_ARGS).longOpt(HOST_NAME).required()
        .argName(HOST_NAME).hasArg().desc("Host Name (required)").build();
    options.addOption(opHost);

    Option opPort = Option.builder(PORT_ARGS).longOpt(PORT_NAME).required()
        .argName(PORT_NAME).hasArg().desc("Port (required)").build();
    options.addOption(opPort);

    Option opUsername = Option.builder(USERNAME_ARGS).longOpt(USERNAME_NAME)
        .required().argName(USERNAME_NAME)
        .hasArg().desc("Username (required)").build();
    options.addOption(opUsername);

    Option opPassword = Option.builder(PASSWORD_ARGS).longOpt(PASSWORD_NAME)
        .optionalArg(true).argName(PASSWORD_NAME).hasArg().desc("Password (optional)").build();
    options.addOption(opPassword);

    Option opFile = Option.builder(FILE_ARGS).required().argName(FILE_NAME).hasArg().desc(
        "If input a file path, load a csv file, "
            + "otherwise load all csv file under this directory (required)")
        .build();
    options.addOption(opFile);

    Option opHelp = Option.builder(HELP_ARGS).longOpt(HELP_ARGS)
        .hasArg(false).desc("Display help information")
        .build();
    options.addOption(opHelp);

    Option opTimeZone = Option.builder(TIME_ZONE_ARGS).argName(TIME_ZONE_NAME).hasArg()
        .desc("Time Zone eg. +08:00 or -01:00 (optional)").build();
    options.addOption(opTimeZone);

    return options;
  }

  /**
   * Data from csv To tsfile.
   */
  private static void loadDataFromCSV(File file, int index) {
    statement = null;
    int fileLine;
    try {
      fileLine = getFileLineCount(file);
    } catch (IOException e) {
      System.out.println("Failed to import file: " + file.getName());
      return;
    }
    File errorFile = new File(errorInsertInfo + index);
    if (!errorFile.exists()) {
      try {
        errorFile.createNewFile();
      } catch (IOException e) {
        System.out.println("Cannot create a errorFile because: " + e.getMessage());
        return;
      }
    }
    System.out.println("Start to import data from: " + file.getName());
    errorFlag = true;
    try(BufferedReader br = new BufferedReader(new FileReader(file));
        BufferedWriter bw = new BufferedWriter(new FileWriter(errorFile));
        ProgressBar pb = new ProgressBar("Import from: " + file.getName(), fileLine)) {
      pb.setExtraMessage("Importing...");
      String header = br.readLine();

      bw.write("From " + file.getAbsolutePath());
      bw.newLine();
      bw.newLine();
      bw.write(header);
      bw.newLine();
      bw.newLine();

      // storage csv table head info
      Map<String, ArrayList<Integer>> deviceToColumn = new HashMap<>();
      // storage csv table head info
      List<String> colInfo = new ArrayList<>();
      // storage csv device sensor info, corresponding csv table head
      List<String> headInfo = new ArrayList<>();

      String[] strHeadInfo = header.split(",");
      if (strHeadInfo.length <= 1) {
        System.out.println("The CSV file "+ file.getName() +" illegal, please check first line");
        return;
      }

      long startTime = System.currentTimeMillis();
      Map<String, String> timeseriesDataType = new HashMap<>();

      boolean success = queryDatabaseMeta(strHeadInfo, file, bw, timeseriesDataType, headInfo,
          deviceToColumn, colInfo);
      if (!success) {
        errorFlag = false;
        return;
      }

      statement = connection.createStatement();


      List<String> tmp = new ArrayList<>();
      success = readAndGenSqls(br, timeseriesDataType, deviceToColumn, colInfo, headInfo,
          bw, tmp, pb);
      if (!success) {
        return;
      }

      executeSqls(bw, tmp, startTime, file);
      pb.stepTo(fileLine);
    } catch (FileNotFoundException e) {
      System.out.println("Cannot find " + file.getName() + " because: "+e.getMessage());
    } catch (IOException e) {
      System.out.println("CSV file read exception because: " + e.getMessage());
    } catch (SQLException e) {
      System.out.println("Database connection exception because: " + e.getMessage());
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
        if (errorFlag) {
          FileUtils.forceDelete(errorFile);
        } else {
          System.out.println("Format of some lines in "+ file.getAbsolutePath() + " error, please "
              + "check "+errorFile.getAbsolutePath()+" for more information");
        }
      } catch (SQLException e) {
        System.out.println("Sql statement can not be closed because: " + e.getMessage());
      } catch (IOException e) {
        System.out.println("Close file error because: " + e.getMessage());
      }
    }
  }

  private static void executeSqls(BufferedWriter bw, List<String> tmp, long startTime, File file)
      throws IOException {
    try {
      int[] result = statement.executeBatch();
      for (int i = 0; i < result.length; i++) {
        if (result[i] != TSStatusCode.SUCCESS_STATUS.getStatusCode() && i < tmp.size()) {
          bw.write(tmp.get(i));
          bw.newLine();
          errorFlag = false;
        }
      }
      statement.clearBatch();
      tmp.clear();
    } catch (SQLException e) {
      bw.write(e.getMessage());
      bw.newLine();
      errorFlag = false;
      System.out.println("Cannot execute sql because: " + e.getMessage());
    }
  }

  private static boolean readAndGenSqls(BufferedReader br, Map<String, String> timeseriesDataType,
      Map<String, ArrayList<Integer>> deviceToColumn, List<String> colInfo,
      List<String> headInfo, BufferedWriter bw, List<String> tmp, ProgressBar pb) throws IOException {
    String line;
    count = 0;
    while ((line = br.readLine()) != null) {
      List<String> sqls;
      try {
        sqls = createInsertSQL(line, timeseriesDataType, deviceToColumn, colInfo, headInfo);
      } catch (Exception e) {
        bw.write(String.format("error input line, maybe it is not complete: %s", line));
        bw.newLine();
        System.out.println("Cannot create sql for " + line + " because: " + e.getMessage());
        errorFlag = false;
        return false;
      }
      boolean success = addSqlsToBatch(sqls, tmp, bw);
      pb.step();
      if (!success) {
        return false;
      }
    }
    return true;
  }

  private static boolean addSqlsToBatch(List<String> sqls, List<String> tmp, BufferedWriter bw)
      throws IOException {
    for (String str : sqls) {
      try {
        count++;
        statement.addBatch(str);
        tmp.add(str);
        checkBatchSize(bw, tmp);

      } catch (SQLException e) {
        bw.write(e.getMessage());
        bw.newLine();
        errorFlag = false;
        System.out.println("Cannot execute sql because: " + e.getMessage());
        return false;
      }
    }
    return true;
  }


  private static void checkBatchSize(BufferedWriter bw, List<String> tmp)
      throws SQLException, IOException {
    if (count == BATCH_EXECUTE_COUNT) {
      int[] result = statement.executeBatch();
      for (int i = 0; i < result.length; i++) {
        if (result[i] != Statement.SUCCESS_NO_INFO && i < tmp.size()) {
          bw.write(tmp.get(i));
          bw.newLine();
          errorFlag = false;
        }
      }
      statement.clearBatch();
      count = 0;
      tmp.clear();
    }
  }

  private static boolean queryDatabaseMeta(String[] strHeadInfo, File file, BufferedWriter bw,
      Map<String, String> timeseriesDataType, List<String> headInfo,
      Map<String, ArrayList<Integer>> deviceToColumn,
      List<String> colInfo)
      throws SQLException, IOException {
    try (Statement statement = connection.createStatement()) {

      for (int i = 1; i < strHeadInfo.length; i++) {
        statement.execute("show timeseries " + strHeadInfo[i]);
        ResultSet resultSet = statement.getResultSet();
        try {
          if (resultSet.next()) {
            /*
             * now the resultSet is like following, so the index of dataType is 4
             * +--------------+-----+-------------+--------+--------+-----------+
               |    timeseries|alias|storage group|dataType|encoding|compression|
               +--------------+-----+-------------+--------+--------+-----------+
               |root.fit.d1.s1| null|  root.fit.d1|   INT32|     RLE|     SNAPPY|
               |root.fit.d1.s2| null|  root.fit.d1|    TEXT|   PLAIN|     SNAPPY|
               +--------------+-----+-------------+--------+--------+-----------+
             */
            timeseriesDataType.put(strHeadInfo[i], resultSet.getString(4));
          } else {
            String errorInfo = String.format("Database cannot find %s in %s, stop import!",
                    strHeadInfo[i], file.getAbsolutePath());
            System.out.println("Database cannot find " + strHeadInfo[i] + " in " + file.getAbsolutePath() + ", "
                    + "stop import!");
            bw.write(errorInfo);
            return false;
          }
        } finally {
          resultSet.close();
        }
        headInfo.add(strHeadInfo[i]);
        String deviceInfo = strHeadInfo[i].substring(0, strHeadInfo[i].lastIndexOf('.'));

        if (!deviceToColumn.containsKey(deviceInfo)) {
          deviceToColumn.put(deviceInfo, new ArrayList<>());
        }
        // storage every device's sensor index info
        deviceToColumn.get(deviceInfo).add(i - 1);
        colInfo.add(strHeadInfo[i].substring(strHeadInfo[i].lastIndexOf('.') + 1));
      }
    }
    return true;
  }

  private static List<String> createInsertSQL(String line, Map<String, String> timeseriesToType,
      Map<String, ArrayList<Integer>> deviceToColumn,
      List<String> colInfo, List<String> headInfo) {
    String[] data = line.split(",", headInfo.size() + 1);
    List<String> sqls = new ArrayList<>();
    Iterator<Map.Entry<String, ArrayList<Integer>>> it = deviceToColumn.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, ArrayList<Integer>> entry = it.next();
      String sql = createOneSql(entry, data, colInfo, timeseriesToType, headInfo);
      if (sql != null) {
        sqls.add(sql);
      }
    }
    return sqls;
  }

  private static String createOneSql(Map.Entry<String, ArrayList<Integer>> entry, String[] data,
      List<String> colInfo, Map<String, String> timeseriesToType, List<String> headInfo) {
    StringBuilder sbd = new StringBuilder();
    ArrayList<Integer> colIndex = entry.getValue();
    sbd.append("insert into ").append(entry.getKey()).append("(timestamp");
    int skipCount = 0;
    for (int j = 0; j < colIndex.size(); ++j) {
      if ("".equals(data[entry.getValue().get(j) + 1])) {
        skipCount++;
        continue;
      }
      sbd.append(", ").append(colInfo.get(colIndex.get(j)));
    }
    // define every device null value' number, if the number equal the
    // sensor number, the insert operation stop
    if (skipCount == entry.getValue().size()) {
      return null;
    }

    // TODO when timestampsStr is empty
    String timestampsStr = data[0];
    sbd.append(") values(").append(timestampsStr.trim().isEmpty()
        ? "NO TIMESTAMP" : timestampsStr);

    for (int j = 0; j < colIndex.size(); ++j) {
      if ("".equals(data[entry.getValue().get(j) + 1])) {
        continue;
      }
      if (timeseriesToType.get(headInfo.get(colIndex.get(j))).equals(STRING_DATA_TYPE)) {
        /**
         * like csv line 1,100,'hello',200,300,400, we will read the third field as 'hello',
         * so, if we add '', the field will be ''hello'', and IoTDB will be failed
         * to insert the field.
         * Now, if we meet the string which is enclosed in quotation marks, we should not add ''
         */
        if ((data[colIndex.get(j) + 1].startsWith("'") && data[colIndex.get(j) + 1].endsWith("'")) ||
                (data[colIndex.get(j) + 1].startsWith("\"") && data[colIndex.get(j) + 1].endsWith("\""))) {
          sbd.append(",").append(data[colIndex.get(j) + 1]);
        } else {
          sbd.append(", \'").append(data[colIndex.get(j) + 1]).append("\'");
        }
      } else {
        sbd.append(",").append(data[colIndex.get(j) + 1]);
      }
    }
    sbd.append(")");
    return sbd.toString();
  }

  public static void main(String[] args) throws IOException, SQLException {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      System.out.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println("Parse error: " + e.getMessage());
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
      String filename = commandLine.getOptionValue(FILE_ARGS);
      if (filename == null) {
        hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
        return;
      }
      parseSpecialParams(commandLine);
      importCsvFromFile(host, port, username, password, filename, timeZoneID);
    } catch (ArgsErrorException e) {
      System.out.println("Args error: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("Encounter an error, because: " + e.getMessage());
    } finally {
      reader.close();
    }
  }

  private static void parseSpecialParams(CommandLine commandLine) {
    timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);
  }

  public static void importCsvFromFile(String ip, String port, String username,
      String password, String filename,
      String timeZone) throws SQLException {
    String property = System.getProperty(IOTDB_CLI_HOME);
    if (property == null) {
      errorInsertInfo = ERROR_INFO_STR;
    } else {
      errorInsertInfo = property + File.separatorChar + ERROR_INFO_STR;
    }
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      connection = (IoTDBConnection) DriverManager.getConnection(Config.IOTDB_URL_PREFIX
              + ip + ":" + port + "/",
          username, password);
      timeZoneID = timeZone;
      setTimeZone();

      File file = new File(filename);
      if (file.isFile()) {
        importFromSingleFile(file);
      } else if (file.isDirectory()) {
        importFromDirectory(file);
      }

    } catch (ClassNotFoundException e) {
      System.out.println("Failed to import data because cannot find IoTDB JDBC Driver, "
          + "please check whether you have imported driver or not: " + e.getMessage());
    } catch (TException e) {
      System.out.println("Encounter an error when connecting to server, because " + e.getMessage());
    } catch (SQLException e){
      System.out.println("Encounter an error when importing data, error is: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("Encounter an error, because: " + e.getMessage());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  private static void importFromSingleFile(File file) {
    if (file.getName().endsWith(FILE_SUFFIX)) {
      loadDataFromCSV(file, 1);
    } else {
      System.out.println("File "+ file.getName() +"  should ends with '.csv' if you want to import");
    }
  }

  private static void importFromDirectory(File file) {
    int i = 1;
    File[] files = file.listFiles();
    if (files == null) {
      return;
    }

    for (File subFile : files) {
      if (subFile.isFile()) {
        if (subFile.getName().endsWith(FILE_SUFFIX)) {
          loadDataFromCSV(subFile, i);
          i++;
        } else {
          System.out.println("File " + file.getName() + " should ends with '.csv' if you want to import");
        }
      }
    }
  }

  private static int getFileLineCount(File file) throws IOException {
    int line = 0;
    try (LineNumberReader count = new LineNumberReader(new FileReader(file))) {
      while (count.skip(Long.MAX_VALUE) > 0) {
        // Loop just in case the file is > Long.MAX_VALUE or skip() decides to not read the entire file
      }
      // +1 because line index starts at 0
      line = count.getLineNumber() + 1;
    }
    return line;
  }
}
