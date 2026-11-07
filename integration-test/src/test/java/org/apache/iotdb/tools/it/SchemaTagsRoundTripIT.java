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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 *
 * <p>The schema of one database and one exported file mixes everything the tools have to keep
 * apart: every data type/encoding/compression combination, all four tag/attribute combinations on
 * different devices, and several combinations across the measurements of one device.
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
    // Awkward tag values, one device per value because tags belong to the device.
    singleTag("v1", "v1", Format.BOOLEAN_PLAIN_SNAPPY);
    singleTag("comma", "a,b", Format.INT32_TS_2DIFF_SNAPPY);
    singleTag("quote", "a\"b", Format.DOUBLE_GORILLA_LZ4);
    singleTag("backslash", "a\\b", Format.INT64_GORILLA_LZ4);
    singleTag("backslashn", "a\\nb", Format.FLOAT_GORILLA_GZIP);
    singleTag("tab", "a\tb", Format.DOUBLE_GORILLA_LZ4);
    singleTag("newline", "a\nb", Format.TEXT_DICTIONARY_SNAPPY);
    singleTag("cjk", "中文😀", Format.INT64_GORILLA_LZ4);
    singleTag("nulllit", "null", Format.BOOLEAN_PLAIN_SNAPPY);
    singleTag("empty", "", Format.INT32_TS_2DIFF_SNAPPY);
    singleTag("backtick", "`x`", Format.FLOAT_GORILLA_GZIP);
    tagged("spacedkey", "k e y", "v", Format.TEXT_DICTIONARY_SNAPPY);

    // The four tag/attribute combinations, each on a device of its own.
    series(
        "both",
        "s1",
        Format.BOOLEAN_PLAIN_SNAPPY,
        mapOf("t1", "v1", "t2", "a,b"),
        mapOf("a1", "one", "a2", "he said \"hi\""),
        null);
    series(
        "multi",
        "s1",
        Format.TEXT_DICTIONARY_SNAPPY,
        mapOf("a", "1", "b", "a,b", "c", "a\"b"),
        NO_VALUES,
        null);
    series(
        "attr",
        "s1",
        Format.TEXT_DICTIONARY_SNAPPY,
        NO_VALUES,
        mapOf("note", "he said \"hi\"", "plain", "v"),
        null);
    series("none", "s1", Format.TEXT_DICTIONARY_SNAPPY, NO_VALUES, NO_VALUES, null);
    series("alias", "s1", Format.FLOAT_GORILLA_GZIP, mapOf("aliasTag", "v"), NO_VALUES, ALIAS);

    // One device mixing three of the combinations across its measurements.
    series(
        "mixed",
        "tagsOnly",
        Format.INT32_TS_2DIFF_SNAPPY,
        mapOf("m1", "x", "m2", "y"),
        NO_VALUES,
        null);
    series(
        "mixed",
        "attrsOnly",
        Format.INT64_GORILLA_LZ4,
        NO_VALUES,
        mapOf("m1", "x", "m2", "y"),
        null);
    series("mixed", "neither", Format.DOUBLE_GORILLA_LZ4, NO_VALUES, NO_VALUES, null);
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
            series.format.dataType,
            series.format.encoding,
            series.format.compression,
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

      final Set<Format> formats = EnumSet.noneOf(Format.class);
      for (Map.Entry<String, Series> entry : CREATED.entrySet()) {
        final String path = entry.getKey();
        final Series series = entry.getValue();
        final CSVRecord record = records.get(path);
        assertNotNull("the export is missing " + path, record);
        assertEquals(path + " DataType", series.format.dataType.name(), record.get("DataType"));
        assertEquals(path + " Encoding", series.format.encoding.name(), record.get("Encoding"));
        assertEquals(
            path + " Compression", series.format.compression.name(), record.get("Compression"));
        assertEquals(
            path + " Alias", series.alias == null ? "" : series.alias, record.get("Alias"));
        assertEquals(path + " Tags", series.tags, parseJsonObject(record.get("Tags")));
        assertEquals(
            path + " Attributes", series.attributes, parseJsonObject(record.get("Attributes")));
        // The three assertions above proved that the file carries this combination.
        formats.add(series.format);
      }
      assertEquals(
          "the file must carry every data type, encoding and compression combination",
          EnumSet.allOf(Format.class),
          formats);

      // Nothing to write is written as an empty cell, not as {} and not as null.
      assertEquals("", records.get(NO_ATTRIBUTES_ROW).get("Tags"));
      assertEquals("", records.get(NO_ATTRIBUTES_ROW).get("Attributes"));

      assertPresenceCombinations(records);
    }
  }

  /**
   * The one exported file must carry all four tag/attribute combinations, spread over several
   * devices, and at least one device must mix several of them across its measurements.
   */
  private static void assertPresenceCombinations(Map<String, CSVRecord> records) {
    final Set<Presence> presences = EnumSet.noneOf(Presence.class);
    final Map<String, Set<Presence>> presencesByDevice = new TreeMap<>();
    for (Map.Entry<String, CSVRecord> entry : records.entrySet()) {
      final String path = entry.getKey();
      final CSVRecord record = entry.getValue();
      final Presence presence =
          Presence.of(
              parseJsonObject(record.get("Tags")), parseJsonObject(record.get("Attributes")));
      presences.add(presence);
      presencesByDevice
          .computeIfAbsent(
              path.substring(0, path.lastIndexOf('.')), device -> EnumSet.noneOf(Presence.class))
          .add(presence);
    }
    assertEquals(
        "the file must carry every tag/attribute combination",
        EnumSet.allOf(Presence.class),
        presences);
    assertTrue(
        "the combinations must be spread over at least four devices: " + presencesByDevice,
        presencesByDevice.size() >= 4);
    assertTrue(
        "one device must mix several combinations across its measurements: " + presencesByDevice,
        presencesByDevice.values().stream().mapToInt(Set::size).max().orElse(0) >= 3);
  }

  private static void assertSchemaRestored()
      throws IoTDBConnectionException, StatementExecutionException {
    final Map<String, Series> restored = readSchema();
    assertEquals("the import must restore every time series", CREATED.keySet(), restored.keySet());
    for (Map.Entry<String, Series> entry : CREATED.entrySet()) {
      final String path = entry.getKey();
      final Series expected = entry.getValue();
      final Series actual = restored.get(path);
      assertEquals(path + " data type/encoding/compression", expected.format, actual.format);
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
      final int dataTypeIndex = columns.indexOf("DataType");
      final int encodingIndex = columns.indexOf("Encoding");
      final int compressionIndex = columns.indexOf("Compression");
      final int tagsIndex = columns.indexOf("Tags");
      final int attributesIndex = columns.indexOf("Attributes");
      assertTrue(
          "SHOW TIMESERIES must expose the Timeseries, Alias, DataType, Encoding, Compression, "
              + "Tags and Attributes columns: "
              + columns,
          timeseriesIndex >= 0
              && aliasIndex >= 0
              && dataTypeIndex >= 0
              && encodingIndex >= 0
              && compressionIndex >= 0
              && tagsIndex >= 0
              && attributesIndex >= 0);
      while (dataSet.hasNext()) {
        final List<Field> fields = dataSet.next().getFields();
        final String path = cell(fields, timeseriesIndex);
        schema.put(
            path,
            new Series(
                Format.of(
                    cell(fields, dataTypeIndex),
                    cell(fields, encodingIndex),
                    cell(fields, compressionIndex)),
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

  private static void singleTag(String device, String value, Format format) {
    tagged(device, "k", value, format);
  }

  private static void tagged(String device, String key, String value, Format format) {
    series(device, "s1", format, mapOf(key, value), NO_VALUES, null);
  }

  private static void series(
      String device,
      String measurement,
      Format format,
      Map<String, String> tags,
      Map<String, String> attributes,
      String alias) {
    CREATED.put(
        DATABASE + "." + device + "." + measurement, new Series(format, tags, attributes, alias));
  }

  private static Map<String, String> mapOf(String... keysAndValues) {
    final Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put(keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  /**
   * A data type, encoding and compression combination of the test schema. Every combination has to
   * appear in the exported file, and the server renders them with these names.
   */
  private enum Format {
    BOOLEAN_PLAIN_SNAPPY(TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY),
    INT32_TS_2DIFF_SNAPPY(TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.SNAPPY),
    INT64_GORILLA_LZ4(TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4),
    DOUBLE_GORILLA_LZ4(TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.LZ4),
    FLOAT_GORILLA_GZIP(TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.GZIP),
    TEXT_DICTIONARY_SNAPPY(TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.SNAPPY);

    private final TSDataType dataType;
    private final TSEncoding encoding;
    private final CompressionType compression;

    Format(TSDataType dataType, TSEncoding encoding, CompressionType compression) {
      this.dataType = dataType;
      this.encoding = encoding;
      this.compression = compression;
    }

    /** Resolves the combination rendered by {@code SHOW TIMESERIES}, i.e. by the export. */
    private static Format of(String dataType, String encoding, String compression) {
      for (Format format : values()) {
        if (format.dataType.name().equals(dataType)
            && format.encoding.name().equals(encoding)
            && format.compression.name().equals(compression)) {
          return format;
        }
      }
      throw new AssertionError(
          "unexpected data type/encoding/compression combination: "
              + dataType
              + "/"
              + encoding
              + "/"
              + compression);
    }
  }

  /** Whether a time series carries tags and attributes; all four must coexist in one file. */
  private enum Presence {
    BOTH,
    TAGS_ONLY,
    ATTRIBUTES_ONLY,
    NEITHER;

    private static Presence of(Map<String, String> tags, Map<String, String> attributes) {
      if (!tags.isEmpty()) {
        return attributes.isEmpty() ? TAGS_ONLY : BOTH;
      }
      return attributes.isEmpty() ? NEITHER : ATTRIBUTES_ONLY;
    }
  }

  /** One time series of the test schema, i.e. one row of the exported file. */
  private static final class Series {
    private final Format format;
    private final Map<String, String> tags;
    private final Map<String, String> attributes;
    private final String alias;

    private Series(
        Format format, Map<String, String> tags, Map<String, String> attributes, String alias) {
      this.format = format;
      this.tags = tags;
      this.attributes = attributes;
      this.alias = alias;
    }
  }
}
