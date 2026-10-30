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

package org.apache.iotdb.tool.data;

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ImportDataStatusTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Map<Field, Object> originalFields = new HashMap<>();
  private final Map<Object, Object> originalPrinters = new HashMap<>();
  private final ByteArrayOutputStream output = new ByteArrayOutputStream();
  private Field printerStream;
  private SessionPool sessionPool;

  @Before
  public void setUp() throws Exception {
    printerStream = IoTPrinter.class.getDeclaredField("screenPrinter");
    printerStream.setAccessible(true);
    for (Class<?> owner :
        List.of(
            AbstractDataTool.class,
            ImportData.class,
            AsyncImportData.class,
            ImportDataScanTool.class)) {
      for (Field field : owner.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) && !Modifier.isFinal(field.getModifiers())) {
          field.setAccessible(true);
          originalFields.put(field, field.get(null));
        }
      }
      if (owner != ImportDataScanTool.class) {
        Field printer = owner.getDeclaredField("ioTPrinter");
        printer.setAccessible(true);
        Object instance = printer.get(null);
        originalPrinters.put(instance, printerStream.get(instance));
        printerStream.set(instance, new PrintStream(output, true, StandardCharsets.UTF_8));
      }
    }
    drainQueue();
    AbstractDataTool.resetCurrentImportFailed();
    AsyncImportData.resetImportStatus();
    AbstractDataTool.loadFileFailedNum.reset();
    AbstractDataTool.loadFileSuccessfulNum.reset();
    AbstractDataTool.failedFileDirectory = null;
    AbstractDataTool.aligned = false;
    AbstractDataTool.host = "127.0.0.1";
    AbstractDataTool.port = "6667";
    AbstractDataTool.username = "root";
    AbstractDataTool.password = "root";
    AbstractDataTool.timeZoneID = null;
    setImportOption("threadNum", 1);
    setImportOption("aligned", false);
    sessionPool = mock(SessionPool.class);
    AsyncImportData.setSessionPool(sessionPool);
  }

  @After
  public void tearDown() throws Exception {
    drainQueue();
    AbstractDataTool.resetCurrentImportFailed();
    AsyncImportData.resetImportStatus();
    AbstractDataTool.loadFileFailedNum.reset();
    AbstractDataTool.loadFileSuccessfulNum.reset();
    for (Map.Entry<Field, Object> entry : originalFields.entrySet()) {
      entry.getKey().set(null, entry.getValue());
    }
    for (Map.Entry<Object, Object> entry : originalPrinters.entrySet()) {
      printerStream.set(entry.getKey(), entry.getValue());
    }
  }

  @Test
  public void workerContinuesAfterFileFailureWithoutPrintingSuccess() throws Exception {
    Path invalid = file("invalid.csv", "Wrong,root.d.s1(INT32)\n1,1\n");
    Path valid = file("valid.csv", "Time,root.d.s1(INT32)\n2,2\n");
    ImportDataScanTool.putToQueue(invalid.toString());
    ImportDataScanTool.putToQueue(valid.toString());

    invokeImport("asyncImportDataFiles");

    assertTrue(AsyncImportData.hasImportFailed());
    assertEquals(1, AbstractDataTool.loadFileFailedNum.sum());
    assertEquals(1, AbstractDataTool.loadFileSuccessfulNum.sum());
    assertEquals(0, ImportDataScanTool.getTsFileQueueSize());
    assertFalse(output.toString(StandardCharsets.UTF_8).contains("Import completely!"));
    verify(sessionPool).insertRecords(anyList(), anyList(), anyList(), anyList(), anyList());
  }

  @Test
  public void asyncMissingSourceReturnsErrorWithoutCreatingSessionPool() throws Exception {
    Path missing = temporaryFolder.getRoot().toPath().resolve("missing");
    setImportOption("targetPath", missing.toString());
    setImportOption("sessionPool", null);

    assertEquals(AbstractDataTool.CODE_ERROR, invokeImport("importFromTargetPathAsync"));
    assertEquals(
        "Source file or directory " + missing + " does not exist" + System.lineSeparator(),
        output.toString(StandardCharsets.UTF_8));
    Field pool = ImportData.class.getDeclaredField("sessionPool");
    pool.setAccessible(true);
    assertNull(pool.get(null));
    assertEquals(0, ImportDataScanTool.getTsFileQueueSize());
  }

  @Test
  public void asyncExitStatusResetsAndConversionFailureRemainsNonfatal() throws Exception {
    Path invalid = file("invalid.csv", "Wrong,root.d.s1(INT32)\n1,1\n");
    setImportOption("targetPath", invalid.toString());
    assertEquals(AbstractDataTool.CODE_ERROR, invokeImport("importFromTargetPathAsync"));
    assertTrue(AsyncImportData.hasImportFailed());
    assertFalse(output.toString(StandardCharsets.UTF_8).contains("Import completely!"));

    Path conversion = file("conversion.csv", "Time,root.d.s1(INT32)\n1,abc\n");
    setImportOption("targetPath", conversion.toString());
    output.reset();
    assertEquals(AbstractDataTool.CODE_OK, invokeImport("importFromTargetPathAsync"));
    assertFalse(AsyncImportData.hasImportFailed());
    assertTrue(output.toString(StandardCharsets.UTF_8).contains("Import completely!"));
    assertEquals(
        "Time,root.d.s1(INT32)\n1,abc\n",
        Files.readString(Path.of(conversion + ".failed_0")).replace("\r\n", "\n"));
  }

  @Test
  public void asyncUnprocessedFilesReturnErrorWithoutPrintingSuccess() throws Exception {
    Path input = file("input.csv", "Time,root.d.s1(INT32)\n1,1\n");
    setImportOption("targetPath", input.toString());
    setImportOption("threadNum", 0);
    assertEquals(AbstractDataTool.CODE_ERROR, invokeImport("importFromTargetPathAsync"));
    assertEquals(1, ImportDataScanTool.getTsFileQueueSize());
    assertFalse(output.toString(StandardCharsets.UTF_8).contains("Import completely!"));
  }

  @Test
  public void syncCsvReturnsErrorAndResetsBeforeNextCommand() throws Exception {
    Session session = mock(Session.class);
    doAnswer(
            invocation -> {
              if (((List<?>) invocation.getArgument(1)).contains(1L)) {
                throw new StatementExecutionException("bad row");
              }
              return null;
            })
        .when(session)
        .insertRecords(anyList(), anyList(), anyList(), anyList(), anyList());
    Path failed = file("failed.csv", "Time,root.d.s1(INT32)\n1,1\n");
    assertEquals(
        AbstractDataTool.CODE_ERROR, ImportData.importFromTargetPath(session, failed.toFile()));
    assertFalse(output.toString(StandardCharsets.UTF_8).contains("Import completely!"));

    output.reset();
    Path valid = file("valid.csv", "Time,root.d.s1(INT32)\n2,2\n");
    assertEquals(
        AbstractDataTool.CODE_OK, ImportData.importFromTargetPath(session, valid.toFile()));
    assertTrue(output.toString(StandardCharsets.UTF_8).contains("Import completely!"));
  }

  @Test
  public void syncSqlReturnsErrorAndKeepsProcessingStatements() throws Exception {
    Session session = mock(Session.class);
    doThrow(new StatementExecutionException("bad statement"))
        .when(session)
        .executeNonQueryStatement("bad");
    Path failed = file("failed.sql", "bad\naccepted\n");
    assertEquals(
        AbstractDataTool.CODE_ERROR, ImportData.importFromTargetPath(session, failed.toFile()));
    verify(session).executeNonQueryStatement("accepted");
    assertEquals("bad\n", Files.readString(Path.of(failed + ".failed")).replace("\r\n", "\n"));
    assertFalse(output.toString(StandardCharsets.UTF_8).contains("Import completely!"));

    output.reset();
    Path valid = file("valid.sql", "valid\n");
    assertEquals(
        AbstractDataTool.CODE_OK, ImportData.importFromTargetPath(session, valid.toFile()));
    assertTrue(output.toString(StandardCharsets.UTF_8).contains("valid.sql Import completely!"));
  }

  @Test
  public void asyncSqlFailureSetsRunStatus() throws Exception {
    doThrow(new StatementExecutionException("bad statement"))
        .when(sessionPool)
        .executeNonQueryStatement("bad");
    Path failed = file("failed.sql", "bad\naccepted\n");
    ImportDataScanTool.putToQueue(failed.toString());

    invokeImport("asyncImportDataFiles");

    assertTrue(AsyncImportData.hasImportFailed());
    assertEquals(1, AbstractDataTool.loadFileFailedNum.sum());
    assertEquals(0, AbstractDataTool.loadFileSuccessfulNum.sum());
    assertFalse(output.toString(StandardCharsets.UTF_8).contains("Import completely!"));
    verify(sessionPool).executeNonQueryStatement("accepted");
  }

  private Path file(String name, String content) throws Exception {
    Path path = temporaryFolder.newFile(name).toPath();
    Files.writeString(path, content);
    return path;
  }

  private static Object invokeImport(String name) throws Exception {
    Method method = ImportData.class.getDeclaredMethod(name);
    method.setAccessible(true);
    return method.invoke(null);
  }

  private static void setImportOption(String name, Object value) throws Exception {
    Field field = ImportData.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(null, value);
  }

  private static void drainQueue() {
    while (ImportDataScanTool.pollFromQueue() != null) {
      // Drain any files left by a failing assertion before restoring shared state.
    }
  }
}
