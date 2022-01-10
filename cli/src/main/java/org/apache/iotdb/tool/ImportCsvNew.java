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

import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.annotation.Nullable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.BOOLEAN;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.FLOAT;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT32;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT64;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.TEXT;

public class ImportCsvNew extends AbstractCsvTool {

  public static Log logger = LogFactory.getLog(ImportCsvNew.class);

  private static final String FILE_ARGS = "f";
  private static final String FILE_NAME = "file or folder";

  private static final String FAILED_FILE_ARGS = "fd";
  private static final String FAILED_FILE_NAME = "failed file directory";

  private static final String CSV_SUFFIXS = ".csv";
  private static final String TXT_SUFFIXS = ".txt";

  private static final String TSFILEDB_CLI_PREFIX = "ImportCsv";

  private static String targetPath;
  private static String failedFileDirectory = null;
  private static List<DateTimeFormatter> fmts = new LinkedList<>();
  private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

  public static final int POINT_FEEDBACK_SIZE = 1_000_000;

  static {
    fmts.add(fmt);
  }

  /**
   * create the commandline options.
   *
   * @return object Options
   */
  private static Options createOptions() {
    Options options = createNewOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .argName(FILE_NAME)
            .hasArg()
            .desc(
                "If input a file path, load a csv file, "
                    + "otherwise load all csv file under this directory (required)")
            .build();
    options.addOption(opFile);

    Option opFailedFile =
        Option.builder(FAILED_FILE_ARGS)
            .argName(FAILED_FILE_NAME)
            .hasArg()
            .desc(
                "Specifying a directory to save failed file, default YOUR_CSV_FILE_PATH (optional)")
            .build();
    options.addOption(opFailedFile);

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opCompress =
        Option.builder(COMPRESS_ARGS)
            .longOpt(COMPRESS_NAME)
            .argName(COMPRESS_NAME)
            .hasArg()
            .desc("Type algorithm for compress, snappy or gzip is available. (optional)")
            .build();
    options.addOption(opCompress);

    return options;
  }

  /**
   * parse optional params
   *
   * @param commandLine
   */
  private static void parseSpecialParams(CommandLine commandLine) {
    timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);
    targetPath = commandLine.getOptionValue(FILE_ARGS);
    if (commandLine.getOptionValue(FAILED_FILE_ARGS) != null) {
      failedFileDirectory = commandLine.getOptionValue(FAILED_FILE_ARGS);
      File file = new File(failedFileDirectory);
      if (!file.isDirectory()) {
        file.mkdir();
        failedFileDirectory = file.getAbsolutePath() + File.separator;
      }
    }
    String compressAlgorithm = commandLine.getOptionValue(COMPRESS_ARGS);
    if (compressAlgorithm == null) {
      if (targetPath.endsWith(".gz")) {
        compressMode = CompressMode.GZIP;
      } else if (targetPath.endsWith(".snappy")) {
        compressMode = CompressMode.SNAPPY;
      } else {
        compressMode = CompressMode.PLAIN;
      }
    } else {
      compressMode = CompressMode.forValue(compressAlgorithm);
    }
  }

  public static void main(String[] args) throws IOException, IoTDBConnectionException {

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
    } catch (org.apache.commons.cli.ParseException e) {
      System.out.println("Parse error: " + e.getMessage());
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      return;
    }

    try {
      parseBasicParams(commandLine);
      String filename = commandLine.getOptionValue(FILE_ARGS);
      if (filename == null) {
        hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
        return;
      }
      parseSpecialParams(commandLine);
    } catch (ArgsErrorException e) {
      System.out.println("Args error: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("Encounter an error, because: " + e.getMessage());
    }

    importFromTargetPath(host, Integer.valueOf(port), username, password, targetPath, timeZoneID);
  }

  /**
   * Specifying a CSV file or a directory including CSV files that you want to import. This method
   * can be offered to console cli to implement importing CSV file by command.
   *
   * @param host
   * @param port
   * @param username
   * @param password
   * @param targetPath a CSV file or a directory including CSV files
   * @param timeZone
   * @throws IoTDBConnectionException
   */
  public static void importFromTargetPath(
      String host, int port, String username, String password, String targetPath, String timeZone)
      throws IoTDBConnectionException {
    try {
      session = new Session(host, Integer.valueOf(port), username, password, false);
      session.open(false);
      timeZoneID = timeZone;
      setTimeZone();

      File file = new File(targetPath);
      if (file.isFile()) {
        importFromSingleFileNew(file);
      } else if (file.isDirectory()) {
        File[] files = file.listFiles();
        if (files == null) {
          return;
        }

        for (File subFile : files) {
          if (subFile.isFile()) {
            importFromSingleFileNew(subFile);
          }
        }
      } else {
        System.out.println("File not found!");
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      System.out.println("Encounter an error when connecting to server, because " + e.getMessage());
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  private static void importFromSingleFileNew(File file) {

    if (CompressMode.PLAIN.equals(compressMode)) {
      if (!file.getName().endsWith(CSV_SUFFIXS) && !file.getName().endsWith(TXT_SUFFIXS)) {
        System.out.println("The file name must end with \".csv\" or \".txt\"!");
        System.exit(0);
      }
    } else if (!file.getName().endsWith(compressMode.getSuffix())) {
      System.out.println("The file name must end with \"" + compressMode.getSuffix() + "\"!");
      System.exit(0);
    }
    try {
      Stream<CSVRecord> csvRecords = readCsvFile(file.getAbsolutePath()).stream();
      writeDataAlignedByTime(csvRecords);
    } catch (IOException e) {
      System.out.println("CSV file read exception because: " + e.getMessage());
    }
  }

  private static void writeDataAlignedByTime(Stream<CSVRecord> records) {

    @SuppressWarnings("unchecked")
    List<String>[] headerNames = new LinkedList[1];

    HashMap<String, List<String>> deviceAndMeasurementNames = new HashMap<>();
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();

    Map<String, CsvModel> m = new HashMap<>();

    boolean[] booleanArray = {
      true, false
    }; // [0]: first or not; [1]: header contains "Device" or not;
    Long[] finishedCount = {0L, 0L}; // 0: finishRowCount; 1: finishPointCountTemp;
    try {
      records.forEach(
          record -> {
            if (booleanArray[0]) {
              booleanArray[0] = false;
              headerNames[0] = new LinkedList<>();
              Map<String, String> map = record.toMap();
              headerNames[0].addAll(map.keySet());

              if (headerNames[0].contains("Device")) {
                booleanArray[1] = true;
              }
              parseHeaders(headerNames[0], deviceAndMeasurementNames, headerTypeMap, headerNameMap);
            }
            int length = deviceAndMeasurementNames.size();
            String d = null;
            if (booleanArray[1]) {
              d = record.get("Device");
            }
            String timeStr = record.get("Time");
            for (Map.Entry<String, List<String>> e : deviceAndMeasurementNames.entrySet()) {
              String measurement = e.getKey();
              String measurementKey =
                  booleanArray[1]
                      ? new StringBuilder(d).append(".").append(measurement).toString()
                      : measurement;
              if (!m.containsKey(measurementKey)) {
                m.put(measurementKey, new CsvModel());
              }
              CsvModel cm = m.get(measurementKey);
              List<String> l = e.getValue();
              List<String> devices = new LinkedList<String>();
              List<TSDataType> types = new LinkedList<TSDataType>();
              List<Object> values = new LinkedList<Object>();
              Long time = 0L;
              if (timeStr.indexOf('+') >= 0) {
                timeStr = timeStr.substring(0, timeStr.indexOf('+'));
              }
              timeStr = timeStr.replaceAll("T", " ");
              if (timeStr.indexOf(' ') == -1) {
                try {
                  time = Long.parseLong(timeStr);
                } catch (Exception e1) {
                }
              } else {
                try {
                  time = fmt.parseDateTime(timeStr).toDate().getTime();
                } catch (Exception e2) {
                }
              }

              for (int i = 0; i < l.size(); i++) {
                if (!measurement.equals(e.getKey())) {
                  continue;
                }
                String ee = l.get(i);
                String header = ee;
                if (!booleanArray[1]) {
                  header = new StringBuilder(e.getKey()).append(".").append(ee).toString();
                }
                String raw = record.get(header);
                if (raw != null && !"".equals(raw)) {
                  String device = ee;
                  if (ee.indexOf('(') > -1) {
                    device = ee.substring(0, ee.indexOf('('));
                  }
                  devices.add(device);
                  TSDataType type = headerTypeMap.get(headerNameMap.get(header));
                  types.add(type);
                  Object value = typeTrans(raw, type);
                  values.add(value);
                } else {
                }
              }
              if (!devices.isEmpty()) {
                if (booleanArray[1]) {
                  cm.setDeviceId(d);
                } else {
                  cm.setDeviceId(e.getKey());
                }
                addLine(
                    cm.getTimes(),
                    cm.getMeasurementsList(),
                    cm.getTypesList(),
                    cm.getValuesList(),
                    time,
                    devices,
                    types,
                    values);
              }
              finishedCount[1]++;
            }
            ongoingFeedback(finishedCount, length);
          });
    } catch (Exception e) {
    }
    for (Map.Entry<String, CsvModel> e2 : m.entrySet()) {
      CsvModel cm = e2.getValue();
      if (cm.getTimes().size() != 0) {
        logger.warn(
            "device " + e2.getKey() + " totally import " + cm.getTimes().size() + " points");
        try {
          session.insertRecordsOfOneDevice(
              cm.getDeviceId(),
              cm.getTimes(),
              cm.getMeasurementsList(),
              cm.getTypesList(),
              cm.getValuesList());
        } catch (IoTDBConnectionException | StatementExecutionException e1) {
        }
      }
    }
    finishFeedback(finishedCount);
  }

  private static void ongoingFeedback(Long[] finishedCount, int length) {
    finishedCount[0]++;
    if (finishedCount[1] >= POINT_FEEDBACK_SIZE) {
      finishedCount[1] = finishedCount[1] % POINT_FEEDBACK_SIZE;
      logger.warn("Imported " + finishedCount[0] + " rows");
    }
  }

  private static void finishFeedback(Long[] finishedCount) {
    logger.warn("Import finish, total " + finishedCount[0] + " rows");
  }

  private static void addLine(
      List<Long> times,
      List<List<String>> measurements,
      List<List<TSDataType>> datatypes,
      List<List<Object>> values,
      long time,
      List<String> s1,
      List<TSDataType> s1type,
      List<Object> value2) {

    List<String> tmpMeasurements = new ArrayList<>();
    List<TSDataType> tmpDataTypes = new ArrayList<>();
    List<Object> tmpValues = new ArrayList<>();
    for (int i = 0; i < s1.size(); i++) {
      tmpMeasurements.add(s1.get(i));
      tmpDataTypes.add(s1type.get(i));
      tmpValues.add(value2.get(i));
    }
    times.add(time);
    measurements.add(tmpMeasurements);
    datatypes.add(tmpDataTypes);
    values.add(tmpValues);
  }

  /**
   * read data from the CSV file
   *
   * @param path
   * @return
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private static CSVParser readCsvFile(String path) throws IOException {
    InputStream input = null;
    if (!CompressMode.PLAIN.equals(compressMode)) {
      input = new PipedInputStream();
      final PipedOutputStream out = new PipedOutputStream((PipedInputStream) input);

      new Thread(
              new Runnable() {
                public void run() {
                  try {
                    if (CompressMode.GZIP.equals(compressMode)) {
                      CompressUtil.gzipUncompress(new FileInputStream(path), out);
                    } else if (CompressMode.SNAPPY.equals(compressMode)) {
                      CompressUtil.snappyUncompress(new FileInputStream(path), out);
                    }
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                }
              })
          .start();
      return CSVFormat.EXCEL
          .withFirstRecordAsHeader()
          .withQuote('\'')
          .withEscape('\\')
          .withIgnoreEmptyLines()
          .parse(new InputStreamReader(input));
    } else {
      input = new FileInputStream(path);
      return CSVFormat.EXCEL
          .withFirstRecordAsHeader()
          .withQuote('\'')
          .withEscape('\\')
          .withIgnoreEmptyLines()
          .parse(new InputStreamReader(input));
    }
  }

  /**
   * parse deviceNames, measurementNames(aligned by time), headerType from headers
   *
   * @param headerNames
   * @param deviceAndMeasurementNames
   * @param headerTypeMap
   * @param headerNameMap
   */
  private static void parseHeaders(
      List<String> headerNames,
      @Nullable HashMap<String, List<String>> deviceAndMeasurementNames,
      HashMap<String, TSDataType> headerTypeMap,
      HashMap<String, String> headerNameMap) {
    String regex = "(?<=\\()\\S+(?=\\))";
    Pattern pattern = Pattern.compile(regex);
    Map<String, String> columnTypeMap = null;
    for (String headerName : headerNames) {
      if (headerName.equals("Time") || headerName.equals("Device")) continue;
      Matcher matcher = pattern.matcher(headerName);
      String type;
      if (matcher.find()) {
        type = matcher.group();
        String headerNameWithoutType =
            headerName.replace("(" + type + ")", "").replaceAll("\\s+", "");
        headerNameMap.put(headerName, headerNameWithoutType);
        headerTypeMap.put(headerNameWithoutType, getType(type));
      } else {
        if (columnTypeMap == null) {
          columnTypeMap = buildColumnTypeMap();
        }
        headerNameMap.put(headerName, headerName);
        headerTypeMap.put(headerName, getType(columnTypeMap.get(headerName)));
      }
      String[] split = headerName.split("\\.");
      String measurementName = split[split.length - 1];
      String deviceName = headerName.replace("." + measurementName, "");
      if (deviceAndMeasurementNames != null) {
        if (!deviceAndMeasurementNames.containsKey(deviceName)) {
          deviceAndMeasurementNames.put(deviceName, new ArrayList<>());
        }
        deviceAndMeasurementNames.get(deviceName).add(measurementName);
      }
    }
  }

  private static Map<String, String> buildColumnTypeMap() {
    Map<String, String> columnTypeMap = new HashMap<>();
    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement("show timeseries");
      DataIterator it = sessionDataSet.iterator();
      while (it.next()) {
        columnTypeMap.put(it.getString("timeseries"), it.getString("dataType"));
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
    }
    return columnTypeMap;
  }

  /**
   * return the TSDataType
   *
   * @param typeStr
   * @return
   */
  private static TSDataType getType(String typeStr) {
    if (typeStr == null) {
      return TEXT;
    }
    switch (typeStr) {
      case "TEXT":
        return TEXT;
      case "BOOLEAN":
        return BOOLEAN;
      case "INT32":
        return INT32;
      case "INT64":
        return INT64;
      case "FLOAT":
        return FLOAT;
      case "DOUBLE":
        return DOUBLE;
      default:
        return TEXT;
    }
  }

  /**
   * @param value
   * @param type
   * @return
   */
  private static Object typeTrans(String value, TSDataType type) {
    try {
      switch (type) {
        case TEXT:
          if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
            value = value.substring(1, value.length() - 1);
            value = value.replaceAll("\"\"", "\"");
          }
          return value;
        case BOOLEAN:
          if (!value.equals("true") && !value.equals("false")) {
            return null;
          }
          return Boolean.valueOf(value);
        case INT32:
          return Integer.valueOf(value);
        case INT64:
          return Long.valueOf(value);
        case FLOAT:
          return Float.valueOf(value);
        case DOUBLE:
          return Double.valueOf(value);
        default:
          return null;
      }
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
