package org.apache.iotdb.tool;

import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jline.reader.LineReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Export CSV file.
 *
 * @version 1.0.0 20170719
 */
public class ExportTsFile extends AbstractCsvTool {

  private static final String TARGET_DIR_ARGS = "td";
  private static final String TARGET_DIR_NAME = "targetDirectory";

  private static final String TARGET_FILE_ARGS = "f";
  private static final String TARGET_FILE_NAME = "targetFile";

  private static final String SQL_FILE_ARGS = "s";
  private static final String SQL_FILE_NAME = "sqlfile";

  private static final String QUERY_COMMAND_ARGS = "q";
  private static final String QUERY_COMMAND_NAME = "queryCommand";

  private static final String TSFILEDB_CLI_PREFIX = "ExportTsFile";

  private static final String DUMP_FILE_NAME_DEFAULT = "dump";
  private static String targetFile = DUMP_FILE_NAME_DEFAULT;

  private static String targetDirectory;

  private static String queryCommand;

  private static final int EXPORT_PER_LINE_COUNT = 10000;

  /** main function of export csv tool. */
  public static void main(String[] args) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      System.out.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    int exitCode = CODE_OK;
    try {
      parseBasicParams(commandLine);
      parseSpecialParams(commandLine);

      session = new Session(host, Integer.parseInt(port), username, password);
      session.open(false);
      setTimeZone();

      if (queryCommand == null) {
        String sqlFile = commandLine.getOptionValue(SQL_FILE_ARGS);
        String sql;

        if (sqlFile == null) {
          LineReader lineReader = JlineUtils.getLineReader(username, host, port);
          sql = lineReader.readLine(TSFILEDB_CLI_PREFIX + "> please input query: ");
          System.out.println(sql);
          String[] values = sql.trim().split(";");
          for (int i = 0; i < values.length; i++) {
            dumpResult(values[i], i);
          }
        } else {
          dumpFromSqlFile(sqlFile);
        }
      } else {
        dumpResult(queryCommand, 0);
      }

    } catch (IOException e) {
      System.out.println("Failed to operate on file, because " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (ArgsErrorException e) {
      System.out.println("Invalid args: " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      System.out.println("Connect failed because " + e.getMessage());
      exitCode = CODE_ERROR;
    } finally {
      if (session != null) {
        try {
          session.close();
        } catch (IoTDBConnectionException e) {
          exitCode = CODE_ERROR;
          System.out.println(
              "Encounter an error when closing session, error is: " + e.getMessage());
        }
      }
    }
    System.exit(exitCode);
  }

  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    targetDirectory = checkRequiredArg(TARGET_DIR_ARGS, TARGET_DIR_NAME, commandLine);
    targetFile = commandLine.getOptionValue(TARGET_FILE_ARGS);
    queryCommand = commandLine.getOptionValue(QUERY_COMMAND_ARGS);

    if (targetFile == null) {
      targetFile = DUMP_FILE_NAME_DEFAULT;
    }

    timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);
    if (!targetDirectory.endsWith("/") && !targetDirectory.endsWith("\\")) {
      targetDirectory += File.separator;
    }
  }

  /**
   * commandline option create.
   *
   * @return object Options
   */
  private static Options createOptions() {
    Options options = createNewOptions();

    Option opTargetFile =
        Option.builder(TARGET_DIR_ARGS)
            .required()
            .argName(TARGET_DIR_NAME)
            .hasArg()
            .desc("Target File Directory (required)")
            .build();
    options.addOption(opTargetFile);

    Option targetFileName =
        Option.builder(TARGET_FILE_ARGS)
            .argName(TARGET_FILE_NAME)
            .hasArg()
            .desc("Export file name (optional)")
            .build();
    options.addOption(targetFileName);

    Option opSqlFile =
        Option.builder(SQL_FILE_ARGS)
            .argName(SQL_FILE_NAME)
            .hasArg()
            .desc("SQL File Path (optional)")
            .build();
    options.addOption(opSqlFile);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opQuery =
        Option.builder(QUERY_COMMAND_ARGS)
            .argName(QUERY_COMMAND_NAME)
            .hasArg()
            .desc("The query command that you want to execute. (optional)")
            .build();
    options.addOption(opQuery);

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    return options;
  }

  /**
   * This method will be called, if the query commands are written in a sql file.
   *
   * @param filePath
   * @throws IOException
   */
  private static void dumpFromSqlFile(String filePath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String sql;
      int index = 0;
      while ((sql = reader.readLine()) != null) {
        dumpResult(sql, index);
        index++;
      }
    }
  }

  /**
   * Dump files from database to TsFile.
   *
   * @param sql export the result of executing the sql
   * @param index used to create dump file name
   */
  private static void dumpResult(String sql, int index) {
    final String path = targetDirectory + targetFile + index + ".tsfile";
    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement(sql);
      writeTsFile(sessionDataSet, path);
      sessionDataSet.closeOperationHandle();
      System.out.println("Export completely!");
    } catch (StatementExecutionException
        | IoTDBConnectionException
        | IOException
        | WriteProcessException e) {
      System.out.println("Cannot dump result because: " + e.getMessage());
    }
  }

  public static void writeTsFile(SessionDataSet sessionDataSet, String filePath)
      throws IOException, IoTDBConnectionException, StatementExecutionException,
          WriteProcessException {
    TsFileWriter tsfilewriter = new TsFileWriter(new File(filePath));
    String deviceId = "out";
    tsfilewriter.registerDevice(filePath, "TsFileExport");

    List<String> names = sessionDataSet.getColumnNames();
    List<String> types = sessionDataSet.getColumnTypes();

    List<MeasurementSchema> schemas = new ArrayList<>();

    // sessionDataSet -> schemas;
    for (int i = 0; i < Array.getLength(names); i++) {
      schemas.add(new MeasurementSchema(names.get(i), TSDataType.valueOf(types.get(i))));
      Map<String, String> props = new HashMap<>();

      // TODO: Fill Map with data from sessionDataSet
      props.put(
          sessionDataSet.iterator().getTimestamp(i).toString(),
          sessionDataSet.iterator().getObject(i).toString());
      // TODO END

      schemas.get(i).setProps(props);
    }

    Tablet tablet = new Tablet(deviceId, schemas);
    tsfilewriter.write(tablet);
  }
}
