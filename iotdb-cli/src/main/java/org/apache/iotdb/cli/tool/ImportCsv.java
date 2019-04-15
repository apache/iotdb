/**
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
package org.apache.iotdb.cli.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
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
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.cli.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * read a CSV formatted data File and insert all the data into IoTDB.
 *
 * @author zhanggr
 */
public class ImportCsv extends AbstractCsvTool {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportCsv.class);

  private static final String FILE_ARGS = "f";
  private static final String FILE_NAME = "file or folder";
  private static final String FILE_SUFFIX = "csv";

  private static final String TSFILEDB_CLI_PREFIX = "ImportCsv";
  private static final String ERROR_INFO_STR = "csvInsertError.error";

  private static final String STRING_DATA_TYPE = "TEXT";
  private static final int BATCH_EXECUTE_COUNT = 10;

  private static String errorInsertInfo = "";
  private static boolean errorFlag;

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

    File errorFile = new File(errorInsertInfo + index);
    if (!errorFile.exists()) {
      try {
        errorFile.createNewFile();
      } catch (IOException e) {
        LOGGER.error("Cannot create a errorFile because, ", e);
        return;
      }
    }

    errorFlag = true;
    try(BufferedReader br = new BufferedReader(new FileReader(file));
    BufferedWriter bw = new BufferedWriter(new FileWriter(errorFile))) {

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
        LOGGER.error("The CSV file {} illegal, please check first line", file.getName());
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
          bw, tmp);
      if (!success) {
        return;
      }

      executeSqls(bw, tmp, startTime, file);

    } catch (FileNotFoundException e) {
      LOGGER.error("Cannot find {}", file.getName(), e);
    } catch (IOException e) {
      LOGGER.error("CSV file read exception! ", e);
    } catch (SQLException e) {
      LOGGER.error("Database connection exception!", e);
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
        if (errorFlag) {
          FileUtils.forceDelete(errorFile);
        } else {
          LOGGER.error("Format of some lines in {} error, please check {} for more "
                  + "information", file.getAbsolutePath(), errorFile.getAbsolutePath());
        }
      } catch (SQLException e) {
        LOGGER.error("Sql statement can not be closed ! ", e);
      } catch (IOException e) {
        LOGGER.error("Close file error ! ", e);
      }
    }
  }

  private static void executeSqls(BufferedWriter bw, List<String> tmp, long startTime, File file)
      throws IOException {
    try {
      int[] result = statement.executeBatch();
      for (int i = 0; i < result.length; i++) {
        if (result[i] != Statement.SUCCESS_NO_INFO && i < tmp.size()) {
          bw.write(tmp.get(i));
          bw.newLine();
          errorFlag = false;
        }
      }
      statement.clearBatch();
      tmp.clear();
      LOGGER.info("Load data from {} successfully, it takes {}ms", file.getName(),
          System.currentTimeMillis() - startTime);
    } catch (SQLException e) {
      bw.write(e.getMessage());
      bw.newLine();
      errorFlag = false;
      LOGGER.error("Cannot execute sql because ", e);
    }
  }

  private static boolean readAndGenSqls(BufferedReader br, Map<String, String> timeseriesDataType,
      Map<String, ArrayList<Integer>> deviceToColumn, List<String> colInfo,
      List<String> headInfo, BufferedWriter bw, List<String> tmp) throws IOException {
    String line;
    count = 0;
    while ((line = br.readLine()) != null) {
      List<String> sqls;
      try {
        sqls = createInsertSQL(line, timeseriesDataType, deviceToColumn, colInfo, headInfo);
      } catch (Exception e) {
        bw.write(String.format("error input line, maybe it is not complete: %s", line));
        bw.newLine();
        LOGGER.error("Cannot create sql for {} because ", line, e);
        errorFlag = false;
        return false;
      }
      boolean success = addSqlsToBatch(sqls, tmp, bw);
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
        LOGGER.error("Cannot execute sql because ", e);
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
    DatabaseMetaData databaseMetaData = connection.getMetaData();

    for (int i = 1; i < strHeadInfo.length; i++) {
      ResultSet resultSet = databaseMetaData.getColumns(Constant.CATALOG_TIMESERIES, strHeadInfo[i], null, null);
      if (resultSet.next()) {
        timeseriesDataType.put(strHeadInfo[i], resultSet.getString(2));
      } else {
        String errorInfo = String.format("Database cannot find %s in %s, stop import!",
            strHeadInfo[i], file.getAbsolutePath());
        LOGGER.error("Database cannot find {} in {}, stop import!",
            strHeadInfo[i], file.getAbsolutePath());
        bw.write(errorInfo);
        return false;
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
        sbd.append(", \'").append(data[colIndex.get(j) + 1]).append("\'");
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
      LOGGER.error("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      LOGGER.error("Parse error ", e);
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
      LOGGER.error("Args error", e);
    } catch (Exception e) {
      LOGGER.error("Encounter an error, because ", e);
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
    String property = System.getProperty("IOTDB_HOME");
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
      LOGGER.error(
          "Failed to dump data because cannot find TsFile JDBC Driver, "
              + "please check whether you have imported driver or not", e);
    } catch (TException e) {
      LOGGER.error("Encounter an error when connecting to server, because ",
          e);
    } catch (Exception e) {
      LOGGER.error("Encounter an error, because ", e);
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
      LOGGER.warn("File {} should ends with '.csv' if you want to import", file.getName());
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
          LOGGER.warn("File {} should ends with '.csv' if you want to import", file.getName());
        }
      }
    }
  }
}
