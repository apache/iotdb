package org.apache.iotdb.tool;

import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import jline.console.ConsoleReader;
import org.apache.commons.cli.*;
import org.apache.commons.csv.*;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.annotation.Nullable;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.*;

public class ImportCsv extends AbstractCsvTool {

  private static final String FILE_ARGS = "f";
  private static final String FILE_NAME = "file or folder";

  private static final String FAILED_FILE_ARGS = "fd";
  private static final String FAILED_FILE_NAME = "failed file directory";

  private static final String CSV_SUFFIXS = "csv";
  private static final String TXT_SUFFIXS = "txt";

  private static final String TSFILEDB_CLI_PREFIX = "ImportCsv";
  private static final String ILLEGAL_PATH_ARGUMENT = "Path parameter is null";

  private static String targetPath;
  private static String failedFileDirectory = null;

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
        failedFileDirectory = file.getAbsolutePath() + "/";
      }
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
    } catch (ArgsErrorException e) {
      System.out.println("Args error: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("Encounter an error, because: " + e.getMessage());
    } finally {
      reader.close();
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
        System.out.println("````````````````````````````````````````````````");
        importFromSingleFile(file);
      } else if (file.isDirectory()) {
        System.out.println("Load file in the directory: " + file.getAbsolutePath());
        System.out.println("````````````````````````````````````````````````");
        File[] files = file.listFiles();
        if (files == null) {
          return;
        }

        for (File subFile : files) {
          if (subFile.isFile()) {
            importFromSingleFile(subFile);
          }
        }
      } else {
        System.out.println("File: " + file.getAbsolutePath() + " not found!!!");
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  /**
   * import the CSV file and load headers and records.
   *
   * @param file the File object of the CSV file that you want to import.
   */
  private static void importFromSingleFile(File file) {
    if (file.getName().endsWith(CSV_SUFFIXS)) {
      System.out.println("Found file: " + file.getName() + '\n' + "Load data from the file ...");
      try {
        CSVParser csvRecords = readCsvFile(file.getAbsolutePath());
        List<String> headerNames = csvRecords.getHeaderNames();
        List<CSVRecord> records = csvRecords.getRecords();
        System.out.println("Total " + records.stream().count() + " records were found.");
        String failedFilePath = null;
        if (failedFileDirectory == null) {
          failedFilePath = file.getAbsolutePath() + ".failed";
        } else {
          failedFilePath = failedFileDirectory + file.getName() + ".failed";
        }
        if (!headerNames.contains("Device")) {
          System.out.println("The data was aligned by time.");
          writeDataAlignedByTime(headerNames, records, failedFilePath);
        } else {
          System.out.println("The data was aligned by device.");
          writeDataAlignedByDevice(headerNames, records, failedFilePath);
        }
        System.out.println("````````````````````````````````````````````````");
      } catch (IOException e) {
        System.out.println("CSV file read exception because: " + e.getMessage());
      }
    }
  }

  /**
   * if the data is aligned by time, the data will be written by this method.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param failedFilePath the directory to save the failed files
   */
  private static void writeDataAlignedByTime(
      List<String> headerNames, List<CSVRecord> records, String failedFilePath) {
    HashMap<String, List<String>> deviceAndMeasurementNames = new HashMap<>();
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, deviceAndMeasurementNames, headerTypeMap, headerNameMap);

    Set<String> devices = deviceAndMeasurementNames.keySet();
    String devicesStr = StringUtils.join(devices, ",");
    try {
      queryType(devicesStr, headerTypeMap, "Time");
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
    }

    if (headerTypeMap.isEmpty()) {
      System.out.println("The type of header was not defined in file or schema.");
    } else {
      System.out.println("The type of header was defined in the file or schema.");
    }

    SimpleDateFormat timeFormatter = formatterInit(records.get(0).get("Time"));

    ArrayList<List<Object>> failedRecords = new ArrayList<>();

    for (Map.Entry<String, List<String>> entry : deviceAndMeasurementNames.entrySet()) {
      String deviceId = entry.getKey();
      List<Long> times = new ArrayList<>();
      List<String> measurementNames = entry.getValue();
      List<List<TSDataType>> typesList = new ArrayList<>();
      List<List<Object>> valuesList = new ArrayList<>();
      List<List<String>> measurementsList = new ArrayList<>();
      System.out.println("Start to load data for device: " + deviceId);
      records.stream()
          .forEach(
              record -> {
                ArrayList<TSDataType> types = new ArrayList<>();
                ArrayList<Object> values = new ArrayList<>();
                ArrayList<String> measurements = new ArrayList<>();
                AtomicReference<Boolean> isFail = new AtomicReference<>(false);
                measurementNames.stream()
                    .forEach(
                        measurementName -> {
                          String header = deviceId + "." + measurementName;
                          String value = record.get(header);
                          if (!value.equals("")) {
                            TSDataType type;
                            if (!headerTypeMap.containsKey(headerNameMap.get(header))) {
                              type = typeInfer(value);
                              headerTypeMap.put(header, type);
                            }
                            type = headerTypeMap.get(headerNameMap.get(header));
                            measurements.add(headerNameMap.get(header).replace(deviceId + '.', ""));
                            types.add(type);
                            Object valueTransed = typeTrans(value, type);
                            if (valueTransed == null) {
                              isFail.set(true);
                            } else {
                              values.add(valueTransed);
                            }
                          }
                        });
                if (isFail.get()) {
                  failedRecords.add(record.stream().collect(Collectors.toList()));
                }
                if (!measurements.isEmpty()) {
                  try {
                    if (timeFormatter == null) {
                      times.add(Long.valueOf(record.get("Time")));
                    } else {
                      times.add(timeFormatter.parse(record.get("Time")).getTime());
                    }
                  } catch (ParseException e) {
                    e.printStackTrace();
                  }
                  typesList.add(types);
                  valuesList.add(values);
                  measurementsList.add(measurements);
                }
              });
      try {
        session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList);
        System.out.println("Insert successfully!!!");
      } catch (StatementExecutionException | IoTDBConnectionException e) {
        System.out.println("Meet error when insert csv because " + e.getMessage());
      }
    }
    if (!failedRecords.isEmpty()) {
      writeCsvFile(headerNames, failedRecords, failedFilePath);
      System.out.println("Failed file path: " + new File(failedFilePath).getAbsolutePath());
    }
  }

  /**
   * if the data is aligned by device, the data will be written by this method.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param failedFilePath the directory to save the failed files
   */
  private static void writeDataAlignedByDevice(
      List<String> headerNames, List<CSVRecord> records, String failedFilePath) {
    HashMap<String, TSDataType> headerTypeMap = new HashMap<>();
    HashMap<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, null, headerTypeMap, headerNameMap);
    Set<String> devices =
        records.stream().map(record -> record.get("Device")).collect(Collectors.toSet());
    String devicesStr = StringUtils.join(devices, ",");
    try {
      queryType(devicesStr, headerTypeMap, "Device");
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
    }

    if (headerTypeMap.isEmpty()) {
      System.out.println("The type of header was not defined in file or schema.");
    } else {
      System.out.println("The type of header was defined in the file or schema.");
    }

    SimpleDateFormat timeFormatter = formatterInit(records.get(0).get("Time"));

    Set<String> measurementNames = headerNameMap.keySet();

    ArrayList<List<Object>> failedRecords = new ArrayList<>();

    devices.stream()
        .forEach(
            device -> {
              List<Long> times = new ArrayList<>();

              List<List<TSDataType>> typesList = new ArrayList<>();
              List<List<Object>> valuesList = new ArrayList<>();
              List<List<String>> measurementsList = new ArrayList<>();

              System.out.println("Start to load data for device: " + device);
              records.stream()
                  .filter(record -> record.get("Device").equals(device))
                  .forEach(
                      record -> {
                        ArrayList<TSDataType> types = new ArrayList<>();
                        ArrayList<Object> values = new ArrayList<>();
                        ArrayList<String> measurements = new ArrayList<>();

                        AtomicReference<Boolean> isFail = new AtomicReference<>(false);

                        measurementNames.stream()
                            .forEach(
                                measurement -> {
                                  String value = record.get(measurement);
                                  if (!value.equals("")) {
                                    TSDataType type;
                                    if (!headerTypeMap.containsKey(
                                        headerNameMap.get(measurement))) {
                                      type = typeInfer(value);
                                      headerTypeMap.put(measurement, type);
                                    }
                                    type = headerTypeMap.get(headerNameMap.get(measurement));
                                    measurements.add(headerNameMap.get(measurement));
                                    types.add(type);
                                    Object valueTransed = typeTrans(value, type);
                                    if (valueTransed == null) {
                                      isFail.set(true);
                                    } else {
                                      values.add(valueTransed);
                                    }
                                  }
                                });
                        if (isFail.get()) {
                          failedRecords.add(record.stream().collect(Collectors.toList()));
                        }
                        if (!measurements.isEmpty()) {
                          try {
                            times.add(timeFormatter.parse(record.get("Time")).getTime());
                          } catch (ParseException e) {
                            e.printStackTrace();
                          }
                          typesList.add(types);
                          valuesList.add(values);
                          measurementsList.add(measurements);
                        }
                      });
              try {
                session.insertRecordsOfOneDevice(
                    device, times, measurementsList, typesList, valuesList);
                System.out.println("Insert successfully!!!");
              } catch (StatementExecutionException | IoTDBConnectionException e) {
                System.out.println("Meet error when insert csv because " + e.getMessage());
              }
            });
    if (!failedRecords.isEmpty()) {
      writeCsvFile(headerNames, failedRecords, failedFilePath);
      System.out.println("Failed file path: " + new File(failedFilePath).getAbsolutePath());
    }
  }

  /**
   * read data from the CSV file
   *
   * @param path
   * @return
   * @throws IOException
   */
  private static CSVParser readCsvFile(String path) throws IOException {
    return CSVFormat.EXCEL
        .withFirstRecordAsHeader()
        .withQuote('\'')
        .withEscape('\\')
        .parse(new InputStreamReader(new FileInputStream(path)));
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
        headerNameMap.put(headerName, headerName);
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

  /**
   * query data type of timeseries from IoTDB
   *
   * @param deviceNames
   * @param headerTypeMap
   * @param alignedType
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   */
  private static void queryType(
      String deviceNames, HashMap<String, TSDataType> headerTypeMap, String alignedType)
      throws IoTDBConnectionException, StatementExecutionException {
    String sql = "select * from " + deviceNames + " limit 1";
    SessionDataSet sessionDataSet = session.executeQueryStatement(sql);
    List<String> columnNames = sessionDataSet.getColumnNames();
    List<String> columnTypes = sessionDataSet.getColumnTypes();
    for (int i = 1; i < columnNames.size(); i++) {
      if (alignedType == "Time") {
        headerTypeMap.put(columnNames.get(i), getType(columnTypes.get(i)));
      } else if (alignedType == "Device") {
        String[] split = columnNames.get(i).split("\\.");
        String measurement = split[split.length - 1];
        headerTypeMap.put(measurement, getType(columnTypes.get(i)));
      }
    }
  }

  /**
   * return a suit time formatter
   *
   * @param time
   * @return
   */
  private static SimpleDateFormat formatterInit(String time) {
    try {
      Long.parseLong(time);
      return null;
    } catch (Exception ignored) {
      // do nothing
    }

    for (String timeFormat : STRING_TIME_FORMAT) {
      SimpleDateFormat format = new SimpleDateFormat(timeFormat);
      try {
        format.parse(time).getTime();
        return format;
      } catch (java.text.ParseException ignored) {
        // do nothing
      }
    }
    return null;
  }

  /**
   * return the TSDataType
   *
   * @param typeStr
   * @return
   */
  private static TSDataType getType(String typeStr) {
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
        return null;
    }
  }

  /**
   * if data type of timeseries is not defined in headers of schema, this method will be called to
   * do type inference
   *
   * @param value
   * @return
   */
  private static TSDataType typeInfer(String value) {
    if (value.contains("\"")) return TEXT;
    else if (value.equals("true") || value.equals("false")) return BOOLEAN;
    else if (!value.contains(".")) {
      try {
        Integer.valueOf(value);
        return INT32;
      } catch (Exception e) {
        return INT64;
      }
    } else {
      if (Float.valueOf(value).toString().length() == Double.valueOf(value).toString().length())
        return FLOAT;
      else return DOUBLE;
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
          return value.substring(1, value.length()-1);
        case BOOLEAN:
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
