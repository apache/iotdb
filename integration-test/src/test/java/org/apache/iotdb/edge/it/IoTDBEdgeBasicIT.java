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

package org.apache.iotdb.edge.it;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.EdgeIT;
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(EdgeIT.class)
public class IoTDBEdgeBasicIT {

  private static final Path WORK_DIR =
      Paths.get("target", "edge-it", IoTDBEdgeBasicIT.class.getSimpleName()).toAbsolutePath();
  private static final Path START_SCRIPT_LOG = WORK_DIR.resolve("start-edge-script.log");
  private static final Path STOP_SCRIPT_LOG = WORK_DIR.resolve("stop-edge-script.log");

  private static final long SCRIPT_TIMEOUT_SECONDS = 45;
  private static final long STARTUP_TIMEOUT_SECONDS = 120;
  private static final Properties PACKAGED_SYSTEM_PROPERTIES = new Properties();

  private static Path edgeHome;
  private static int[] ports;
  private static int rpcPort;
  private static long edgePid = -1;

  @BeforeClass
  public static void setUp() throws Exception {
    deleteRecursively(WORK_DIR);
    Files.createDirectories(WORK_DIR);

    final String packageProperty = System.getProperty("EdgePackage");
    assertTrue(
        "The EdgePackage system property must point to the Edge zip", packageProperty != null);
    final Path edgePackage = Paths.get(packageProperty).toAbsolutePath().normalize();
    assertTrue("Edge package does not exist: " + edgePackage, Files.isRegularFile(edgePackage));

    final Path extractionDir = WORK_DIR.resolve("package");
    unzip(edgePackage, extractionDir);
    edgeHome = findEdgeHome(extractionDir);
    try (InputStream input =
        Files.newInputStream(edgeHome.resolve("conf/iotdb-system.properties"))) {
      PACKAGED_SYSTEM_PROPERTIES.load(input);
    }

    ports = EnvUtils.searchAvailablePorts();
    rpcPort = ports[2];
    configurePorts(edgeHome.resolve("conf/iotdb-system.properties"));

    runScript(edgeHome.resolve("sbin/start-edge.sh"), START_SCRIPT_LOG);
    edgePid = Long.parseLong(Files.readString(edgeHome.resolve("edge.pid")).trim());
    waitUntilReady();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    AssertionError stopFailure = null;
    try {
      if (edgeHome != null && Files.isRegularFile(edgeHome.resolve("sbin/stop-edge.sh"))) {
        try {
          runScript(edgeHome.resolve("sbin/stop-edge.sh"), STOP_SCRIPT_LOG);
        } catch (Exception | AssertionError e) {
          stopFailure = new AssertionError("Failed to stop IoTDB Edge with stop-edge.sh", e);
        }
      }
    } finally {
      stopProcessForciblyIfNeeded();
      if (ports != null) {
        Files.deleteIfExists(Paths.get(EnvUtils.getLockFilePath(ports[0])));
      }
    }
    if (stopFailure != null) {
      throw stopFailure;
    }
  }

  @Test
  public void testTreeModelReadWrite() throws SQLException {
    try (Connection connection = openTreeConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.edge_it");
      statement.execute(
          "CREATE TIMESERIES root.edge_it.device.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.edge_it.device(time,s1) VALUES (1,42), (2,84)");

      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1 FROM root.edge_it.device ORDER BY TIME")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertEquals(42, resultSet.getInt(2));
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getLong(1));
        assertEquals(84, resultSet.getInt(2));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testTableModelReadWrite() throws SQLException {
    try (Connection connection = openTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE edge_it_table");
      statement.execute("USE edge_it_table");
      statement.execute("CREATE TABLE sensor(device STRING TAG, value INT32 FIELD)");
      statement.execute("INSERT INTO sensor(time,device,value) VALUES (1,'d1',42), (2,'d2',84)");

      try (ResultSet resultSet =
          statement.executeQuery("SELECT device, value FROM sensor ORDER BY time")) {
        assertTrue(resultSet.next());
        assertEquals("d1", resultSet.getString(1));
        assertEquals(42, resultSet.getInt(2));
        assertTrue(resultSet.next());
        assertEquals("d2", resultSet.getString(1));
        assertEquals(84, resultSet.getInt(2));
        assertFalse(resultSet.next());
      }
    }
  }

  private static Connection openTreeConnection() throws SQLException {
    return DriverManager.getConnection(
        jdbcUrl(), SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
  }

  private static Connection openTableConnection() throws SQLException {
    return DriverManager.getConnection(
        jdbcUrl() + "?sql_dialect=table",
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD);
  }

  @Test
  public void testPackagedConfiguration() throws Exception {
    assertFalse(PACKAGED_SYSTEM_PROPERTIES.containsKey("model_inference_execution_thread_count"));
    assertEdgeProperty("candidate_compaction_task_queue_size", "10");
    assertEdgeProperty("compaction_max_aligned_series_num_in_one_batch", "2");
    assertEdgeProperty("target_compaction_file_size", "10485760");
    assertEdgeProperty("inner_compaction_total_file_size_threshold", "52428800");
    assertEdgeProperty("inner_compaction_total_file_num_threshold", "10");
    assertEdgeProperty("inner_compaction_candidate_file_num", "5");
    assertEdgeProperty("max_cross_compaction_candidate_file_num", "10");
    assertEdgeProperty("max_cross_compaction_candidate_file_size", "52428800");
    assertEdgeProperty("target_chunk_point_num", "10000");
    assertEdgeProperty("target_chunk_size", "262144");
    assertEdgeProperty("max_number_of_points_in_page", "1000");
    assertEdgeProperty("page_size_in_byte", "16384");
    assertTrue(Files.isRegularFile(edgeHome.resolve("sbin/windows/check-edge.ps1")));

    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    final Document document;
    try (InputStream input = Files.newInputStream(edgeHome.resolve("conf/logback-edge.xml"))) {
      document = factory.newDocumentBuilder().parse(input);
    }
    final Set<String> appenderNames = new HashSet<>();
    final NodeList appenders = document.getElementsByTagName("appender");
    for (int i = 0; i < appenders.getLength(); i++) {
      appenderNames.add(((Element) appenders.item(i)).getAttribute("name"));
    }
    final NodeList references = document.getElementsByTagName("appender-ref");
    for (int i = 0; i < references.getLength(); i++) {
      final String name = ((Element) references.item(i)).getAttribute("ref");
      assertTrue("Undefined Edge log appender: " + name, appenderNames.contains(name));
    }
  }

  private static void assertEdgeProperty(final String name, final String expectedValue) {
    assertEquals(
        "Unexpected packaged Edge value for " + name,
        expectedValue,
        PACKAGED_SYSTEM_PROPERTIES.getProperty(name));
  }

  private static String jdbcUrl() {
    return Config.IOTDB_URL_PREFIX + "127.0.0.1:" + rpcPort;
  }

  private static void configurePorts(final Path configFile) throws IOException {
    final String configNodeAddress = System.getProperty("EdgeConfigNodeAddress", "127.0.0.1");
    final Map<String, String> replacements = new LinkedHashMap<>();
    replacements.put("cn_seed_config_node", configNodeAddress + ":" + ports[0]);
    replacements.put("dn_seed_config_node", configNodeAddress + ":" + ports[0]);
    replacements.put("cn_internal_address", configNodeAddress);
    replacements.put("cn_internal_port", Integer.toString(ports[0]));
    replacements.put("cn_consensus_port", Integer.toString(ports[1]));
    replacements.put("dn_rpc_address", "127.0.0.1");
    replacements.put("dn_rpc_port", Integer.toString(ports[2]));
    replacements.put("dn_internal_address", "127.0.0.1");
    replacements.put("dn_internal_port", Integer.toString(ports[3]));
    replacements.put("dn_mpp_data_exchange_port", Integer.toString(ports[4]));
    replacements.put("dn_schema_region_consensus_port", Integer.toString(ports[5]));
    replacements.put("dn_data_region_consensus_port", Integer.toString(ports[6]));
    replacements.put("cn_metric_prometheus_reporter_port", Integer.toString(ports[7]));
    replacements.put("dn_metric_prometheus_reporter_port", Integer.toString(ports[8]));

    final Set<String> replacedKeys = new HashSet<>();
    final List<String> configuredLines = new ArrayList<>();
    for (final String line : Files.readAllLines(configFile, StandardCharsets.UTF_8)) {
      final int separatorIndex = line.indexOf('=');
      final String key = separatorIndex < 0 ? line : line.substring(0, separatorIndex).trim();
      if (replacements.containsKey(key)) {
        configuredLines.add(key + "=" + replacements.get(key));
        replacedKeys.add(key);
      } else {
        configuredLines.add(line);
      }
    }
    if (!replacedKeys.equals(replacements.keySet())) {
      final Set<String> missingKeys = new HashSet<>(replacements.keySet());
      missingKeys.removeAll(replacedKeys);
      throw new IOException("Missing Edge configuration properties: " + missingKeys);
    }
    Files.write(configFile, configuredLines, StandardCharsets.UTF_8);
  }

  private static void waitUntilReady() throws Exception {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(STARTUP_TIMEOUT_SECONDS);
    SQLException lastException = null;
    while (System.nanoTime() < deadline) {
      if (!ProcessHandle.of(edgePid).map(ProcessHandle::isAlive).orElse(false)) {
        break;
      }
      try (Connection connection = openTreeConnection();
          Statement statement = connection.createStatement();
          ResultSet ignored = statement.executeQuery("SHOW DATABASES")) {
        return;
      } catch (SQLException e) {
        lastException = e;
      }
      Thread.sleep(1000);
    }

    final Path consoleLog = edgeHome.resolve("logs/log_edge_console.log");
    throw new AssertionError(
        "IoTDB Edge did not become ready. Last JDBC error: "
            + lastException
            + System.lineSeparator()
            + readLogTail(consoleLog));
  }

  private static void runScript(final Path script, final Path outputFile) throws Exception {
    Files.createDirectories(outputFile.getParent());
    final ProcessBuilder processBuilder = new ProcessBuilder("bash", script.toString());
    processBuilder.directory(edgeHome.toFile());
    processBuilder.redirectErrorStream(true);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(outputFile.toFile()));
    processBuilder.environment().put("IOTDB_HOME", edgeHome.toString());
    processBuilder.environment().put("IOTDB_CONF", edgeHome.resolve("conf").toString());
    processBuilder.environment().put("IOTDB_DATA_HOME", edgeHome.toString());
    processBuilder.environment().put("IOTDB_LOG_DIR", edgeHome.resolve("logs").toString());

    final Process process = processBuilder.start();
    if (!process.waitFor(SCRIPT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      throw new AssertionError(
          "Timed out running " + script + System.lineSeparator() + readLogTail(outputFile));
    }
    if (process.exitValue() != 0) {
      throw new AssertionError(
          script
              + " exited with code "
              + process.exitValue()
              + System.lineSeparator()
              + readLogTail(outputFile));
    }
  }

  private static void stopProcessForciblyIfNeeded() throws InterruptedException {
    if (edgePid <= 0) {
      return;
    }
    final ProcessHandle process = ProcessHandle.of(edgePid).orElse(null);
    if (process == null || !process.isAlive()) {
      return;
    }
    process.destroy();
    for (int i = 0; i < 10 && process.isAlive(); i++) {
      Thread.sleep(1000);
    }
    if (process.isAlive()) {
      process.destroyForcibly();
    }
  }

  private static Path findEdgeHome(final Path extractionDir) throws IOException {
    try (Stream<Path> paths = Files.list(extractionDir)) {
      final List<Path> directories = paths.filter(Files::isDirectory).collect(Collectors.toList());
      if (directories.size() != 1) {
        throw new IOException(
            "Expected one top-level directory in the Edge package, but found " + directories);
      }
      return directories.get(0);
    }
  }

  private static void unzip(final Path zipFile, final Path destination) throws IOException {
    Files.createDirectories(destination);
    try (ZipInputStream input = new ZipInputStream(Files.newInputStream(zipFile))) {
      ZipEntry entry;
      while ((entry = input.getNextEntry()) != null) {
        final Path output = destination.resolve(entry.getName()).normalize();
        if (!output.startsWith(destination)) {
          throw new IOException("Zip entry escapes the extraction directory: " + entry.getName());
        }
        if (entry.isDirectory()) {
          Files.createDirectories(output);
        } else {
          Files.createDirectories(output.getParent());
          Files.copy(input, output, StandardCopyOption.REPLACE_EXISTING);
        }
        input.closeEntry();
      }
    }
  }

  private static String readLogTail(final Path logFile) {
    if (!Files.isRegularFile(logFile)) {
      return "Log file does not exist: " + logFile;
    }
    try {
      final List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
      return lines.stream()
          .skip(Math.max(0, lines.size() - 200))
          .collect(Collectors.joining(System.lineSeparator()));
    } catch (IOException e) {
      return "Could not read log file " + logFile + ": " + e;
    }
  }

  private static void deleteRecursively(final Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(directory)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }
}
