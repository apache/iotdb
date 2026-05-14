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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TsFileBackupIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.sshd.common.file.nativefs.NativeFileSystemFactory;
import org.apache.sshd.scp.server.ScpCommandFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.shell.ProcessShellCommandFactory;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(IoTDBTestRunner.class)
@Category({TsFileBackupIT.class})
public class TsFileBackupScriptIT {

  private static final String IOTDB_USER = "root";
  private static final String IOTDB_PASSWORD = "TimechoDB@2021";
  private static final String PIPE_PLUGIN_NAME = "TSFILE_REMOTE_SINK";
  private static final String PIPE_PLUGIN_JAR_PREFIX = "tsfile-remote-sink-";
  private static final String PIPE_PLUGIN_JAR_SUFFIX = "-jar-with-dependencies.jar";
  private static final String TEST_SSH_USER = "tsfilebackup";
  private static final String TEST_SSH_PASSWORD = "tsfilebackup";

  private static final String TABLE_T1 = "t1";
  private static final String TABLE_T2 = "t2";
  private static final String TREE_D1 = "d1";
  private static final String TREE_D2 = "d2";

  private static final int HISTORY_INSERT_START = 1;
  private static final int HISTORY_INSERT_ROWS = 100;
  private static final int DELETE_OFFSET = 20;
  private static final int DELETE_LENGTH = 25;
  private static final long PROCESS_TIMEOUT_SECONDS = 60;
  private static final long VERIFY_TIMEOUT_SECONDS = 120;

  private static BaseEnv senderEnv;
  private static BaseEnv receiverEnv;
  private static String ip;
  private static String port;
  private static String toolsPath;
  private static String libPath;
  private static Path artifactDir;
  private static SshServer scpServer;
  private static ExecutorService processOutputExecutor;

  @BeforeClass
  public static void setUp() throws Exception {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    senderEnv.initClusterEnvironment();

    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    receiverEnv.initClusterEnvironment();

    ip = senderEnv.getIP();
    port = senderEnv.getPort();
    toolsPath = senderEnv.getToolsPath();
    libPath = senderEnv.getLibPath();
    artifactDir = Paths.get(toolsPath).getParent().getParent().resolve("tsfile-backup-it");
    Files.createDirectories(artifactDir);

    copyPluginJarToToolHome();
    scpServer = startScpServer(artifactDir);
    processOutputExecutor = Executors.newCachedThreadPool();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (processOutputExecutor != null) {
      processOutputExecutor.shutdownNow();
    }
    if (scpServer != null) {
      scpServer.stop(true);
      scpServer = null;
    }
    if (senderEnv != null) {
      senderEnv.cleanClusterEnvironment();
    }
    if (receiverEnv != null) {
      receiverEnv.cleanClusterEnvironment();
    }
  }

  @Test
  public void testTableBackupSpecificTable() throws Exception {
    assertUnixLikeEnvironment();
    final String dbName = "table_spec_" + generateRandomSuffix();

    prepareTableData(senderEnv, dbName);

    final Path testRoot = Files.createTempDirectory(artifactDir, "table-spec-");
    final Path remoteDir = testRoot.resolve("remote");
    String pipeName = null;

    try {
      List<String> extraArgs =
          Arrays.asList("-sql_dialect", "table", "-db", dbName, "-table", TABLE_T1);
      ProcessResult result = executeBackupScript(remoteDir, extraArgs);
      assertProcessSuccess(senderEnv, result);
      pipeName = extractPipeName(result.outputLines);

      waitForExportCompleted(senderEnv, pipeName, remoteDir);
      loadExportedFiles(receiverEnv, remoteDir, dbName);

      verifyTableLoadedState(
          receiverEnv,
          dbName,
          Collections.singletonList(TABLE_T1),
          Collections.singletonList(TABLE_T2));
    } finally {
      dropPipeIfExists(senderEnv, pipeName);
    }
  }

  @Test
  public void testTableBackupDatabaseOnly() throws Exception {
    assertUnixLikeEnvironment();
    final String dbName = "table_db_" + generateRandomSuffix();

    prepareTableData(senderEnv, dbName);

    final Path testRoot = Files.createTempDirectory(artifactDir, "table-db-");
    final Path remoteDir = testRoot.resolve("remote");
    String pipeName = null;

    try {
      List<String> extraArgs = Arrays.asList("-sql_dialect", "table", "-db", dbName);
      ProcessResult result = executeBackupScript(remoteDir, extraArgs);
      assertProcessSuccess(senderEnv, result);
      pipeName = extractPipeName(result.outputLines);

      waitForExportCompleted(senderEnv, pipeName, remoteDir);
      loadExportedFiles(receiverEnv, remoteDir, dbName);

      verifyTableLoadedState(
          receiverEnv, dbName, Arrays.asList(TABLE_T1, TABLE_T2), Collections.emptyList());
    } finally {
      dropPipeIfExists(senderEnv, pipeName);
    }
  }

  @Test
  public void testTreeBackupSpecificPath() throws Exception {
    assertUnixLikeEnvironment();
    final String dbName = "root.tree_path_" + generateRandomSuffix();

    prepareTreeData(senderEnv, dbName);

    final Path testRoot = Files.createTempDirectory(artifactDir, "tree-path-");
    final Path remoteDir = testRoot.resolve("remote");
    String pipeName = null;

    try {
      String targetPath = dbName + "." + TREE_D1 + ".**";
      List<String> extraArgs = Arrays.asList("-sql_dialect", "tree", "-path", targetPath);
      ProcessResult result = executeBackupScript(remoteDir, extraArgs);
      assertProcessSuccess(senderEnv, result);
      pipeName = extractPipeName(result.outputLines);

      waitForExportCompleted(senderEnv, pipeName, remoteDir);
      loadExportedFilesTree(receiverEnv, remoteDir, dbName);

      verifyTreeLoadedState(
          receiverEnv,
          dbName,
          Collections.singletonList(TREE_D1),
          Collections.singletonList(TREE_D2));
    } finally {
      dropPipeIfExists(senderEnv, pipeName);
    }
  }

  private static String generateRandomSuffix() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }

  private static void assertUnixLikeEnvironment() {
    final String osName = System.getProperty("os.name", "").toLowerCase(Locale.ENGLISH);
    if (osName.startsWith("windows")) {
      throw new IllegalStateException("TsFile backup SCP IT only supports Unix-like environments.");
    }
  }

  private static void copyPluginJarToToolHome() throws IOException {
    final Path pluginJar = locatePluginJar();
    final Path extPipeDir = Paths.get(toolsPath).getParent().resolve("ext").resolve("pipe");
    Files.createDirectories(extPipeDir);
    Files.copy(
        pluginJar,
        extPipeDir.resolve(pluginJar.getFileName()),
        StandardCopyOption.REPLACE_EXISTING);
  }

  private static Path locatePluginJar() throws IOException {
    Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
    while (current != null) {
      final Path targetDir =
          current.resolve("library-pipe").resolve("tsfile-remote-sink").resolve("target");
      if (Files.isDirectory(targetDir)) {
        try (Stream<Path> stream = Files.list(targetDir)) {
          return stream
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().startsWith(PIPE_PLUGIN_JAR_PREFIX))
              .filter(path -> path.getFileName().toString().endsWith(PIPE_PLUGIN_JAR_SUFFIX))
              .min(Comparator.comparing(path -> path.getFileName().toString()))
              .orElseThrow(() -> new IllegalStateException("Cannot find plugin jar"));
        }
      }
      current = current.getParent();
    }
    throw new IllegalStateException("Cannot locate plugin jar");
  }

  private static void prepareTableData(BaseEnv env, String dbName) throws Exception {
    try (ITableSession session = env.getTableSessionConnection()) {
      session.executeNonQueryStatement(String.format("DROP DATABASE IF EXISTS %s", dbName));
      session.executeNonQueryStatement(String.format("CREATE DATABASE IF NOT EXISTS %s", dbName));
      session.executeNonQueryStatement(String.format("USE %s", dbName));
      session.executeNonQueryStatement(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s (id STRING TAG, file OBJECT FIELD)", TABLE_T1));
      session.executeNonQueryStatement(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s (id STRING TAG, file OBJECT FIELD)", TABLE_T2));

      insertTableRows(session, TABLE_T1);
      insertTableRows(session, TABLE_T2);
      TestUtils.executeNonQueryWithRetry(env, "flush");

      final long deleteStart = HISTORY_INSERT_START + DELETE_OFFSET;
      final long deleteEnd = deleteStart + DELETE_LENGTH - 1L;
      session.executeNonQueryStatement(
          String.format(
              "DELETE FROM %s WHERE time >= %d AND time <= %d", TABLE_T1, deleteStart, deleteEnd));
      session.executeNonQueryStatement(
          String.format(
              "DELETE FROM %s WHERE time >= %d AND time <= %d", TABLE_T2, deleteStart, deleteEnd));
      TestUtils.executeNonQueryWithRetry(env, "flush");
    }
  }

  private static void prepareTreeData(BaseEnv env, String dbName) throws Exception {
    try (ISession session = env.getSessionConnection()) {
      session.open();
      dropTreeDatabaseIfExists(session, dbName);
      session.executeNonQueryStatement(String.format("CREATE DATABASE %s", dbName));
      session.executeNonQueryStatement(
          String.format(
              "CREATE TIMESERIES %s.%s.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN", dbName, TREE_D1));
      session.executeNonQueryStatement(
          String.format(
              "CREATE TIMESERIES %s.%s.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN", dbName, TREE_D2));

      insertTreeRows(session, dbName + "." + TREE_D1);
      insertTreeRows(session, dbName + "." + TREE_D2);
      TestUtils.executeNonQueryWithRetry(env, "flush");

      final long deleteStart = HISTORY_INSERT_START + DELETE_OFFSET;
      final long deleteEnd = deleteStart + DELETE_LENGTH - 1L;
      session.executeNonQueryStatement(
          String.format(
              "DELETE FROM %s.%s.s1 WHERE time >= %d AND time <= %d",
              dbName, TREE_D1, deleteStart, deleteEnd));
      session.executeNonQueryStatement(
          String.format(
              "DELETE FROM %s.%s.s1 WHERE time >= %d AND time <= %d",
              dbName, TREE_D2, deleteStart, deleteEnd));
      TestUtils.executeNonQueryWithRetry(env, "flush");
    }
  }

  private static SshServer startScpServer(final Path serverRoot) throws IOException {
    final SshServer server = SshServer.setUpDefaultServer();
    server.setHost("127.0.0.1");
    server.setPort(0);
    server.setKeyPairProvider(
        new SimpleGeneratorHostKeyProvider(serverRoot.resolve("hostkey.ser")));
    server.setPasswordAuthenticator(
        (username, password, session) ->
            TEST_SSH_USER.equals(username) && TEST_SSH_PASSWORD.equals(password));
    server.setFileSystemFactory(new NativeFileSystemFactory());

    final ScpCommandFactory scpCommandFactory = new ScpCommandFactory();
    scpCommandFactory.setDelegateCommandFactory(ProcessShellCommandFactory.INSTANCE);
    server.setCommandFactory(scpCommandFactory);
    server.setShellFactory(scpCommandFactory);
    server.start();
    return server;
  }

  private ProcessResult executeBackupScript(final Path remoteDir, final List<String> extraArgs)
      throws Exception {
    List<String> command =
        new ArrayList<>(
            Arrays.asList(
                "bash",
                Paths.get(toolsPath, "tsfile-backup.sh").toString(),
                "-h",
                ip,
                "-p",
                port,
                "-u",
                IOTDB_USER,
                "-pw",
                IOTDB_PASSWORD,
                "-t",
                remoteDir.toString(),
                "-th",
                "127.0.0.1",
                "-tu",
                TEST_SSH_USER,
                "-tpw",
                TEST_SSH_PASSWORD,
                "-tp",
                String.valueOf(scpServer.getPort())));
    command.addAll(extraArgs);

    final ProcessBuilder builder = new ProcessBuilder(command);
    builder.redirectErrorStream(true);
    builder.environment().put("CLASSPATH", libPath);

    final Process process = builder.start();

    Future<List<String>> outputFuture =
        processOutputExecutor.submit(
            () -> {
              List<String> lines = new ArrayList<>();
              try (BufferedReader reader =
                  new BufferedReader(
                      new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                  lines.add(line);
                }
              }
              return lines;
            });

    boolean finished = process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (!finished) {
      process.destroyForcibly();
      throw new TimeoutException("Backup script timeout");
    }

    List<String> outputLines;
    try {
      outputLines = outputFuture.get(5, TimeUnit.SECONDS);
    } catch (ExecutionException | TimeoutException e) {
      outputLines = Collections.emptyList();
    }

    return new ProcessResult(process.exitValue(), outputLines);
  }

  private void assertProcessSuccess(BaseEnv env, ProcessResult result)
      throws IoTDBConnectionException, StatementExecutionException {
    String output = String.join(System.lineSeparator(), result.outputLines);
    Assert.assertEquals("Script failed: " + output, 0, result.exitCode);
    Assert.assertTrue("Pipe not submitted: " + output, output.contains("Pipe task submitted"));
    Assert.assertTrue("Plugin not registered", isPluginRegistered(env));
  }

  private static boolean isPluginRegistered(BaseEnv env)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = env.getSessionConnection()) {
      session.open();
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW PIPEPLUGINS")) {
        final SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          final String pluginName = readPluginName(iterator);
          if (PIPE_PLUGIN_NAME.equals(pluginName)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static String readPluginName(final SessionDataSet.DataIterator iterator) {
    try {
      if (!iterator.isNull(ColumnHeaderConstant.PLUGIN_NAME)) {
        return iterator.getString(ColumnHeaderConstant.PLUGIN_NAME);
      }
    } catch (Exception ignored) {
    }
    try {
      if (!iterator.isNull(ColumnHeaderConstant.PLUGIN_NAME_TABLE_MODEL)) {
        return iterator.getString(ColumnHeaderConstant.PLUGIN_NAME_TABLE_MODEL);
      }
    } catch (Exception ignored) {
    }
    return null;
  }

  private static String extractPipeName(final List<String> outputLines) {
    for (String line : outputLines) {
      final String prefix = "[INFO] Pipe name: ";
      final int index = line.indexOf(prefix);
      if (index >= 0) {
        return line.substring(index + prefix.length()).trim();
      }
    }
    return null;
  }

  private static void waitForExportCompleted(
      BaseEnv env, final String pipeName, final Path remoteDir) {
    try {
      if (pipeName != null && !pipeName.isEmpty()) {
        Awaitility.await()
            .atMost(Duration.ofSeconds(VERIFY_TIMEOUT_SECONDS))
            .pollInterval(Duration.ofSeconds(1))
            .until(() -> !isPipePresent(env, pipeName));
      }

      Awaitility.await()
          .atMost(Duration.ofSeconds(PROCESS_TIMEOUT_SECONDS))
          .pollInterval(Duration.ofSeconds(1))
          .until(() -> containsTsFile(remoteDir));
    } catch (Exception e) {
      throw new RuntimeException("Failed waiting for export completion.", e);
    }
  }

  private static boolean isPipePresent(BaseEnv env, final String pipeName) throws Exception {
    try (ISession session = env.getSessionConnection()) {
      session.open();
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW PIPES")) {
        final SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          if (pipeName.equals(iterator.getString("ID"))) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static void dropPipeIfExists(BaseEnv env, final String pipeName) {
    if (pipeName == null || pipeName.isEmpty()) {
      return;
    }
    try (ISession session = env.getSessionConnection()) {
      session.open();
      session.executeNonQueryStatement("DROP PIPE " + pipeName);
    } catch (Exception ignored) {
    }
  }

  private static void loadExportedFiles(BaseEnv env, final Path dir, String targetDbName)
      throws Exception {
    if (!Files.exists(dir)) {
      return;
    }
    try (ITableSession session = env.getTableSessionConnection();
        Stream<Path> pathStream = Files.walk(dir)) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS " + targetDbName);
      session.executeNonQueryStatement("USE " + targetDbName);

      List<Path> tsFiles =
          pathStream
              .filter(Files::isRegularFile)
              .filter(p -> p.toString().endsWith(".tsfile"))
              .sorted()
              .collect(Collectors.toList());

      for (Path tsFile : tsFiles) {
        session.executeNonQueryStatement(
            String.format(
                "LOAD '%s' WITH ('database'='%s')",
                tsFile.toAbsolutePath().toString(), targetDbName));
      }
      TestUtils.executeNonQueryWithRetry(env, "flush");
    }
  }

  private static void loadExportedFilesTree(BaseEnv env, final Path dir, String targetDbName)
      throws Exception {
    if (!Files.exists(dir)) {
      return;
    }
    try (ISession session = env.getSessionConnection();
        Stream<Path> pathStream = Files.walk(dir)) {
      session.open();
      session.executeNonQueryStatement("CREATE DATABASE " + targetDbName);

      List<Path> tsFiles =
          pathStream
              .filter(Files::isRegularFile)
              .filter(p -> p.toString().endsWith(".tsfile"))
              .sorted()
              .collect(Collectors.toList());

      for (Path tsFile : tsFiles) {
        session.executeNonQueryStatement(
            String.format(
                "LOAD '%s' WITH ('database'='%s')",
                tsFile.toAbsolutePath().toString(), targetDbName));
      }
      TestUtils.executeNonQueryWithRetry(env, "flush");
    }
  }

  private void verifyTableLoadedState(
      BaseEnv env, final String dbName, List<String> expectedTables, List<String> excludedTables) {
    final List<Long> expectedTimestamps = buildExpectedTimestamps();
    try (ITableSession session = env.getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + dbName);
      for (String table : expectedTables) {
        assertTableLoadedResult(session, table, expectedTimestamps);
      }
      for (String table : excludedTables) {
        assertTableEmptyOrNotExist(session, table);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void verifyTreeLoadedState(
      BaseEnv env,
      final String dbName,
      List<String> expectedDevices,
      List<String> excludedDevices) {
    final List<Long> expectedTimestamps = buildExpectedTimestamps();
    try (ISession session = env.getSessionConnection()) {
      session.open();
      for (String device : expectedDevices) {
        assertTreeLoadedResult(session, dbName + "." + device, expectedTimestamps);
      }
      for (String device : excludedDevices) {
        assertTreeEmptyOrNotExist(session, dbName + "." + device);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void dropTreeDatabaseIfExists(final ISession session, final String database)
      throws Exception {
    try {
      session.executeNonQueryStatement("DROP DATABASE " + database);
    } catch (Exception e) {
      if (e.getMessage() == null
          || (!e.getMessage().contains("does not exist")
              && !e.getMessage().contains("has not been created")
              && !e.getMessage().contains("Path ["))) {
        throw e;
      }
    }
  }

  private static List<Long> buildExpectedTimestamps() {
    final List<Long> expected = new ArrayList<>();
    final long deleteStart = HISTORY_INSERT_START + DELETE_OFFSET;
    final long deleteEnd = deleteStart + DELETE_LENGTH - 1L;
    for (long ts = HISTORY_INSERT_START; ts < HISTORY_INSERT_START + HISTORY_INSERT_ROWS; ts++) {
      if (ts < deleteStart || ts > deleteEnd) {
        expected.add(ts);
      }
    }
    return expected;
  }

  private static void assertTableLoadedResult(
      final ITableSession session, final String tableName, final List<Long> expectedTimestamps)
      throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            String.format("SELECT time, READ_OBJECT(file) FROM %s ORDER BY time ASC", tableName))) {
      final SessionDataSet.DataIterator iterator = dataSet.iterator();
      int index = 0;
      while (iterator.next()) {
        final Binary blob = iterator.getBlob(2);
        if (blob == null || blob.getValues() == null) {
          continue;
        }
        Assert.assertTrue(index < expectedTimestamps.size());
        final long expectedTimestamp = expectedTimestamps.get(index);
        Assert.assertEquals(expectedTimestamp, iterator.getLong(1));
        Assert.assertArrayEquals(buildPayload(expectedTimestamp), blob.getValues());
        index++;
      }
      Assert.assertEquals(expectedTimestamps.size(), index);
    }
  }

  private static void assertTableEmptyOrNotExist(
      final ITableSession session, final String tableName) throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(String.format("SELECT time FROM %s", tableName))) {
      Assert.assertFalse(dataSet.iterator().next());
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("does not exist") || e.getMessage().contains("not exist"));
    }
  }

  private static void assertTreeLoadedResult(
      final ISession session, final String fullDevicePath, final List<Long> expectedTimestamps)
      throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            String.format("SELECT s1 FROM %s ORDER BY time ASC", fullDevicePath))) {
      final SessionDataSet.DataIterator iterator = dataSet.iterator();
      int index = 0;
      while (iterator.next()) {
        Assert.assertTrue(index < expectedTimestamps.size());
        final long expectedTimestamp = expectedTimestamps.get(index);
        Assert.assertEquals(expectedTimestamp, iterator.getLong(1));
        Assert.assertEquals(
            new String(buildPayload(expectedTimestamp), StandardCharsets.UTF_8),
            iterator.getString(2));
        index++;
      }
      Assert.assertEquals(expectedTimestamps.size(), index);
    }
  }

  private static void assertTreeEmptyOrNotExist(final ISession session, final String fullDevicePath)
      throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(String.format("SELECT s1 FROM %s", fullDevicePath))) {
      Assert.assertFalse(dataSet.iterator().next());
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("does not exist")
              || e.getMessage().contains("has not been created"));
    }
  }

  private static void insertTableRows(final ITableSession session, final String tableName)
      throws Exception {
    final List<String> columnNames = Arrays.asList("id", "file");
    final List<TSDataType> dataTypes = Arrays.asList(TSDataType.STRING, TSDataType.OBJECT);
    final List<ColumnCategory> columnCategories =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

    final Tablet tablet = new Tablet(tableName, columnNames, dataTypes, columnCategories, 100);
    for (long ts = HISTORY_INSERT_START; ts < HISTORY_INSERT_START + HISTORY_INSERT_ROWS; ts++) {
      final int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, ts);
      tablet.addValue(rowIndex, 0, "device1");
      tablet.addValue(rowIndex, 1, true, 0, buildPayload(ts));
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insert(tablet);
        tablet.reset();
      }
    }
    if (tablet.getRowSize() > 0) {
      session.insert(tablet);
    }
  }

  private static void insertTreeRows(final ISession session, final String devicePath)
      throws Exception {
    List<String> measurements = Collections.singletonList("s1");
    List<TSDataType> dataTypes = Collections.singletonList(TSDataType.TEXT);

    Tablet tablet = new Tablet(new StringArrayDeviceID(devicePath), measurements, dataTypes, 100);
    for (long ts = HISTORY_INSERT_START; ts < HISTORY_INSERT_START + HISTORY_INSERT_ROWS; ts++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, ts);
      tablet.addValue(rowIndex, 0, new String(buildPayload(ts), StandardCharsets.UTF_8));
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }
    if (tablet.getRowSize() > 0) {
      session.insertTablet(tablet);
    }
  }

  private static byte[] buildPayload(final long timestamp) {
    return ("Payload_" + timestamp).getBytes(StandardCharsets.UTF_8);
  }

  private static boolean containsTsFile(final Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return false;
    }
    try (Stream<Path> stream = Files.walk(dir)) {
      return stream
          .filter(Files::isRegularFile)
          .anyMatch(path -> path.getFileName().toString().endsWith(".tsfile"));
    }
  }

  private static final class ProcessResult {
    private final int exitCode;
    private final List<String> outputLines;

    private ProcessResult(final int exitCode, final List<String> outputLines) {
      this.exitCode = exitCode;
      this.outputLines = outputLines;
    }
  }
}
