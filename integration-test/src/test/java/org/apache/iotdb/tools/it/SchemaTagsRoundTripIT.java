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
package org.apache.iotdb.tools.it;

import org.apache.iotdb.cli.it.AbstractScriptIT;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tool.common.Constants;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Field;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Proves that the tree model schema tools restore the tags and attributes of every time series:
 * {@code export-schema} writes them as RFC4180 quoted JSON, {@code import-schema} reads that format
 * back, and awkward characters survive the round trip unchanged.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class SchemaTagsRoundTripIT extends AbstractScriptIT {

  private static final String DATABASE = "root.rt";
  private static final String EXPORT_PREFIX = "schema_tags";
  private static final String HEADER =
      "Timeseries,Alias,DataType,Encoding,Compression,Tags,Attributes";
  private static final String ALIAS = "aliased";

  /** The row whose tag value carries a double quote, i.e. the row that must be RFC4180 quoted. */
  private static final String QUOTE_ROW = DATABASE + ".quote.s1";

  /** The JSON object the export writes for the tag value {@code a"b}. */
  private static final String QUOTE_ROW_TAGS_JSON = "{\"k\":\"a\\\"b\"}";

  private static final String NO_ATTRIBUTES_ROW = DATABASE + ".none.s1";

  /** How long the metadata drop may take to become visible on every node. */
  private static final int SCHEMA_CLEANUP_TIMEOUT_SECONDS = 60;

  private static final Type JSON_MAP_TYPE = new TypeToken<Map<String, String>>() {}.getType();
  private static final Gson GSON = new Gson();
  private static final Map<String, String> NO_VALUES = Collections.emptyMap();

  /** Time series path to the tags, attributes and alias the test creates for it. */
  private static final Map<String, Series> CREATED = new LinkedHashMap<>();

  static {
    // one tag value per device, because tags belong to the device rather than to the measurement
    singleTag("v1", "v1");
    singleTag("comma", "a,b");
    singleTag("quote", "a\"b");
    singleTag("backslash", "a\\b");
    singleTag("backslashn", "a\\nb");
    singleTag("tab", "a\tb");
    singleTag("newline", "a\nb");
    singleTag("cjk", "中文😀");
    singleTag("nulllit", "null");
    singleTag("empty", "");
    singleTag("backtick", "`x`");
    tagged("spacedkey", "k e y", "v");
    CREATED.put(
        DATABASE + ".multi.s1",
        new Series(mapOf("a", "1", "b", "a,b", "c", "a\"b"), NO_VALUES, null));
    CREATED.put(
        DATABASE + ".attr.s1",
        new Series(NO_VALUES, mapOf("note", "he said \"hi\"", "plain", "v"), null));
    CREATED.put(NO_ATTRIBUTES_ROW, new Series(NO_VALUES, NO_VALUES, null));
    CREATED.put(DATABASE + ".alias.s1", new Series(mapOf("aliasTag", "v"), NO_VALUES, ALIAS));
  }

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private static String ip;
  private static String port;
  private static String toolsPath;
  private static String homePath;

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().initClusterEnvironment();
    ip = EnvFactory.getEnv().getIP();
    port = EnvFactory.getEnv().getPort();
    toolsPath = EnvFactory.getEnv().getToolsPath();
    final String libPath = EnvFactory.getEnv().getLibPath();
    homePath =
        libPath.substring(0, libPath.lastIndexOf(File.separator + "lib" + File.separator + "*"));
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() throws IOException {
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      testOnWindows();
    } else {
      testOnUnix();
    }
  }

  @Override
  protected void testOnWindows() throws IOException {
    try {
      roundTrip(true);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void testOnUnix() throws IOException {
    try {
      roundTrip(false);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Exports the schema, drops it, imports the exported file again and compares the tags and
   * attributes read back from the server with the ones that were created.
   */
  private void roundTrip(boolean windows) throws Exception {
    final Path outputDirectory = folder.newFolder("export").toPath();

    createSchema();

    testOutput(
        exportProcessBuilder(windows, outputDirectory),
        new String[] {Constants.EXPORT_COMPLETELY},
        0);

    final Path csv = singleExportFile(outputDirectory);
    assertExportFormat(csv);

    dropSchema();

    testOutput(importProcessBuilder(windows, csv), new String[] {Constants.IMPORT_COMPLETELY}, 0);

    assertSchemaRestored();
  }

  private static void createSchema() throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.createDatabase(DATABASE);
      for (Map.Entry<String, Series> entry : CREATED.entrySet()) {
        final Series series = entry.getValue();
        // Set the tags and attributes through the session API, so that no SQL escaping can hide a
        // character that the export or the import tool mangles.
        session.createTimeseries(
            entry.getKey(),
            TSDataType.DOUBLE,
            TSEncoding.GORILLA,
            CompressionType.LZ4,
            null,
            series.tags.isEmpty() ? null : series.tags,
            series.attributes.isEmpty() ? null : series.attributes,
            series.alias);
      }
    }
  }

  private static void dropSchema() throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("DROP DATABASE " + DATABASE);
    }
    // The drop reaches the DataNodes asynchronously. Waiting until it is observable keeps a
    // leftover device from making the import pass without restoring anything.
    Awaitility.await()
        .atMost(SCHEMA_CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> !databaseExists() && readSchema().isEmpty());
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // Importing a file only creates time series, so the database has to exist beforehand.
      session.createDatabase(DATABASE);
    }
  }

  private static boolean databaseExists()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection();
        SessionDataSet dataSet = session.executeQueryStatement("SHOW DATABASES")) {
      while (dataSet.hasNext()) {
        if (DATABASE.equals(cell(dataSet.next().getFields(), 0))) {
          return true;
        }
      }
    }
    return false;
  }

  private Path singleExportFile(Path outputDirectory) throws IOException {
    try (Stream<Path> files = Files.list(outputDirectory)) {
      final List<Path> csvFiles =
          files
              .filter(file -> file.getFileName().toString().startsWith(EXPORT_PREFIX))
              .filter(file -> file.getFileName().toString().endsWith(".csv"))
              .collect(Collectors.toList());
      assertEquals("expected exactly one exported CSV file, found " + csvFiles, 1, csvFiles.size());
      return csvFiles.get(0);
    }
  }

  private static void assertExportFormat(Path csv) throws IOException {
    final List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
    assertTrue("the exported schema file is empty", lines.size() > 1);
    assertEquals(HEADER, lines.get(0));

    final String content = Files.readString(csv, StandardCharsets.UTF_8);
    // RFC4180 uses LF as the record separator; a CR would come from a platform dependent writer.
    assertFalse("the export must use LF line endings", content.contains("\r"));
    // RFC4180: the JSON object of the value containing a double quote is wrapped in a quoted cell
    // and every inner quote is doubled, instead of being escaped with a backslash.
    final String quotedCell = "\"" + QUOTE_ROW_TAGS_JSON.replace("\"", "\"\"") + "\"";
    assertTrue(
        "the Tags cell of "
            + QUOTE_ROW
            + " is not RFC4180 quoted: "
            + quotedCell
            + " in\n"
            + content,
        content.contains(quotedCell));

    try (CSVParser parser = CSVParser.parse(csv, StandardCharsets.UTF_8, csvFormat())) {
      final Map<String, CSVRecord> records = new TreeMap<>();
      for (CSVRecord record : parser) {
        records.put(record.get("Timeseries"), record);
      }
      assertEquals(
          "unexpected number of exported rows: " + records, CREATED.size(), records.size());

      for (Map.Entry<String, Series> entry : CREATED.entrySet()) {
        final String path = entry.getKey();
        final Series series = entry.getValue();
        final CSVRecord record = records.get(path);
        assertNotNull("the export is missing " + path, record);
        assertEquals(path + " DataType", "DOUBLE", record.get("DataType"));
        assertEquals(path + " Encoding", "GORILLA", record.get("Encoding"));
        assertEquals(path + " Compression", "LZ4", record.get("Compression"));
        assertEquals(
            path + " Alias", series.alias == null ? "" : series.alias, record.get("Alias"));
        assertEquals(path + " Tags", series.tags, parseJsonObject(record.get("Tags")));
        assertEquals(
            path + " Attributes", series.attributes, parseJsonObject(record.get("Attributes")));
      }

      // Nothing to write is written as an empty cell, not as {} and not as null.
      assertEquals("", records.get(NO_ATTRIBUTES_ROW).get("Tags"));
      assertEquals("", records.get(NO_ATTRIBUTES_ROW).get("Attributes"));
    }
  }

  private static void assertSchemaRestored()
      throws IoTDBConnectionException, StatementExecutionException {
    final Map<String, Series> restored = readSchema();
    assertEquals("the import must restore every time series", CREATED.keySet(), restored.keySet());
    for (Map.Entry<String, Series> entry : CREATED.entrySet()) {
      final String path = entry.getKey();
      final Series expected = entry.getValue();
      final Series actual = restored.get(path);
      assertEquals(path + " tags", expected.tags, actual.tags);
      assertEquals(path + " attributes", expected.attributes, actual.attributes);
      assertEquals(path + " alias", expected.alias, actual.alias);
    }
  }

  /** Reads the tags, attributes and alias of every time series of the test database. */
  private static Map<String, Series> readSchema()
      throws IoTDBConnectionException, StatementExecutionException {
    final Map<String, Series> schema = new TreeMap<>();
    try (ISession session = EnvFactory.getEnv().getSessionConnection();
        SessionDataSet dataSet =
            session.executeQueryStatement("SHOW TIMESERIES " + DATABASE + ".**")) {
      final List<String> columns = dataSet.getColumnNames();
      final int timeseriesIndex = columns.indexOf("Timeseries");
      final int aliasIndex = columns.indexOf("Alias");
      final int tagsIndex = columns.indexOf("Tags");
      final int attributesIndex = columns.indexOf("Attributes");
      assertTrue(
          "SHOW TIMESERIES must expose the Timeseries, Alias, Tags and Attributes columns: "
              + columns,
          timeseriesIndex >= 0 && aliasIndex >= 0 && tagsIndex >= 0 && attributesIndex >= 0);
      while (dataSet.hasNext()) {
        final List<Field> fields = dataSet.next().getFields();
        final String path = cell(fields, timeseriesIndex);
        schema.put(
            path,
            new Series(
                parseJsonObject(cell(fields, tagsIndex)),
                parseJsonObject(cell(fields, attributesIndex)),
                cell(fields, aliasIndex)));
      }
    }
    return schema;
  }

  /** Returns a cell of a schema row, or {@code null} when the server sent a NULL there. */
  private static String cell(List<Field> fields, int index) {
    if (index < 0 || index >= fields.size()) {
      return null;
    }
    final Field field = fields.get(index);
    if (field == null) {
      return null;
    }
    // A NULL field is rendered as the text "null" by getStringValue(), which is how the export
    // tool distinguishes a missing value as well.
    final String value = field.getStringValue();
    return "null".equals(value) ? null : value;
  }

  /** Parses a JSON object cell into a map; an empty cell carries no tags and no attributes. */
  private static Map<String, String> parseJsonObject(String cell) {
    if (cell == null || cell.trim().isEmpty()) {
      return new TreeMap<>();
    }
    final Map<String, String> parsed = GSON.fromJson(cell, JSON_MAP_TYPE);
    return parsed == null ? new TreeMap<>() : new TreeMap<>(parsed);
  }

  private static CSVFormat csvFormat() {
    return CSVFormat.Builder.create(CSVFormat.DEFAULT)
        .setHeader()
        .setSkipHeaderRecord(true)
        .build();
  }

  private ProcessBuilder exportProcessBuilder(boolean windows, Path outputDirectory) {
    final List<String> command = new ArrayList<>();
    addScriptInvocation(command, windows, "export-schema");
    addConnectionArguments(command);
    command.addAll(
        Arrays.asList(
            "-t",
            outputDirectory.toAbsolutePath().toString(),
            "-pfn",
            EXPORT_PREFIX,
            "-path",
            DATABASE + ".**"));
    return processBuilder(command, windows);
  }

  private ProcessBuilder importProcessBuilder(boolean windows, Path csv) {
    final List<String> command = new ArrayList<>();
    addScriptInvocation(command, windows, "import-schema");
    addConnectionArguments(command);
    command.addAll(Arrays.asList("-db", DATABASE, "-s", csv.toAbsolutePath().toString()));
    return processBuilder(command, windows);
  }

  private static void addConnectionArguments(List<String> command) {
    command.addAll(
        Arrays.asList(
            "-h",
            ip,
            "-p",
            port,
            "-u",
            SessionConfig.DEFAULT_USER,
            "-pw",
            SessionConfig.DEFAULT_PASSWORD));
  }

  private static void addScriptInvocation(List<String> command, boolean windows, String script) {
    if (windows) {
      command.add("cmd.exe");
      command.add("/c");
      command.add(
          toolsPath
              + File.separator
              + "windows"
              + File.separator
              + "schema"
              + File.separator
              + script
              + ".bat");
    } else {
      command.add("bash");
      command.add(toolsPath + File.separator + "schema" + File.separator + script + ".sh");
    }
  }

  private static ProcessBuilder processBuilder(List<String> command, boolean windows) {
    if (windows) {
      // cmd.exe exits with the error level of the script it ran.
      command.add("&");
      command.add("exit");
      command.add("%^errorlevel%");
    }
    final ProcessBuilder builder = new ProcessBuilder(command);
    builder.environment().put("IOTDB_HOME", homePath);
    return builder;
  }

  private static void singleTag(String device, String value) {
    tagged(device, "k", value);
  }

  private static void tagged(String device, String key, String value) {
    CREATED.put(DATABASE + "." + device + ".s1", new Series(mapOf(key, value), NO_VALUES, null));
  }

  private static Map<String, String> mapOf(String... keysAndValues) {
    final Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put(keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  /** The tags, attributes and alias created for one time series. */
  private static final class Series {
    private final Map<String, String> tags;
    private final Map<String, String> attributes;
    private final String alias;

    private Series(Map<String, String> tags, Map<String, String> attributes, String alias) {
      this.tags = tags;
      this.attributes = attributes;
      this.alias = alias;
    }
  }
}
