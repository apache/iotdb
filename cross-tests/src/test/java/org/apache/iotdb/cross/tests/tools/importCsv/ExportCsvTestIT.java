package org.apache.iotdb.cross.tests.tools.importCsv;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExportCsvTestIT extends AbstractScript {

  @Before
  public void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Override
  protected void testOnWindows(String queryCommand, String[] output) throws IOException {
    String dir = getCliPath();
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            dir + File.separator + "tools" + File.separator + "export-csv.bat",
            "-h",
            "127.0.0.1",
            "-p",
            "6667",
            "-u",
            "root",
            "-pw",
            "root",
            "-td",
            "./target",
            "-q",
            queryCommand);
    testOutput(builder, output);
  }

  @Override
  protected void testOnUnix(String queryCommand, String[] output) throws IOException {
    String dir = getCliPath();
    ProcessBuilder builder =
        new ProcessBuilder(
            "sh",
            dir + File.separator + "tools" + File.separator + "export-csv.sh",
            "-h",
            "127.0.0.1",
            "-p",
            "6667",
            "-u",
            "root",
            "-pw",
            "root",
            "-td",
            "./target",
            "-s",
            queryCommand);
    testOutput(builder, output);
  }

  @Test
  public void testExport()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String queryCommand = "select c1,c2,c3 from root.test.t1";
    prepareData();
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      testOnWindows(queryCommand, null);
    } else {
      testOnUnix(queryCommand, null);
    }
    CSVParser parser = readCsvFile("./target/dump0.csv");
    String headers = "Time,root.test.t1.c1,root.test.t1.c2,root.test.t1.c3";
    assertEquals(StringUtils.join(parser.getHeaderNames(), ','), headers);
    ArrayList<String> realRecords = new ArrayList<>(Arrays.asList("1.0,\"\"abc\",aa\",\"abbe's\""));
    List<CSVRecord> records = parser.getRecords();
    for (int i = 0; i < records.size(); i++) {
      String record = StringUtils.join(records.get(i).toList(), ',');
      String recordWithoutTime = record.substring(record.indexOf(',') + 1);
      assertEquals(realRecords.get(i), recordWithoutTime);
    }
  }

  @Test
  public void testAggregationQuery()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String queryCommand = "select count(c1),count(c2),count(c3) from root.test.t1";
    prepareData();
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      testOnWindows(queryCommand, null);
    } else {
      testOnUnix(queryCommand, null);
    }
    CSVParser parser = readCsvFile("./target/dump0.csv");
    String headers = "count(root.test.t1.c1),count(root.test.t1.c2),count(root.test.t1.c3)";
    assertEquals(StringUtils.join(parser.getHeaderNames(), ','), headers);
    ArrayList<String> realRecords = new ArrayList<>(Arrays.asList("1,1,1"));
    List<CSVRecord> records = parser.getRecords();
    for (int i = 0; i < records.size(); i++) {
      String record = StringUtils.join(records.get(i).toList(), ',');
      assertEquals(realRecords.get(i), record);
    }
  }

  private void prepareData() throws IoTDBConnectionException, StatementExecutionException {
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.test.t1";
    List<String> measurements = new ArrayList<>();
    measurements.add("c1");
    measurements.add("c2");
    measurements.add("c3");

    List<String> values = new ArrayList<>();
    values.add("1.0");
    values.add("\"abc\",aa");
    values.add("abbe's");
    session.insertRecord(deviceId, 1L, measurements, values);
  }

  private static CSVParser readCsvFile(String path) throws IOException {
    return CSVFormat.EXCEL
        .withFirstRecordAsHeader()
        .withQuote('\'')
        .withEscape('\\')
        .parse(new InputStreamReader(new FileInputStream(path)));
  }
}
