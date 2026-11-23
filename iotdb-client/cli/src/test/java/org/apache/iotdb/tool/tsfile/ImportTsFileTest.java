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

package org.apache.iotdb.tool.tsfile;

import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.common.ImportTsFileOperation;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ImportTsFileTest extends AbstractTsFileTool {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Map<Field, Object> originalFields = new HashMap<>();
  private SessionPool sessionPool;
  private Path input;
  private Path archive;

  @Before
  public void setUp() throws Exception {
    for (Class<?> owner :
        List.of(
            ImportTsFile.class,
            ImportTsFileBase.class,
            ImportTsFileLocally.class,
            ImportTsFileRemotely.class,
            ImportTsFileScanTool.class)) {
      for (Field field : owner.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) && !Modifier.isFinal(field.getModifiers())) {
          field.setAccessible(true);
          originalFields.put(field, field.get(null));
        }
      }
    }
    sessionPool = mock(SessionPool.class);
    ImportTsFileLocally.setSessionPool(sessionPool);
    setImportOption("isRemoteLoad", false);
    setImportOption("threadNum", 2);
    input = Files.createDirectories(temporaryFolder.getRoot().toPath().resolve("input"));
    archive = Files.createDirectories(temporaryFolder.getRoot().toPath().resolve("archive"));
    ImportTsFileScanTool.setSourceFullPath(input.toString());
    ImportTsFileBase.setSuccessAndFailDirAndOperation(
        archive.toString(), ImportTsFileOperation.CP, archive.toString(), ImportTsFileOperation.CP);
  }

  @After
  public void tearDown() throws Exception {
    while (ImportTsFileScanTool.pollFromQueue() != null) {
      // Drain any files left by a failing assertion before restoring the shared state.
    }
    ImportTsFileBase.resetCounters();
    for (Map.Entry<Field, Object> entry : originalFields.entrySet()) {
      entry.getKey().set(null, entry.getValue());
    }
  }

  @Test
  public void testSuccessfulImportReturnsZero() throws Exception {
    Path source = createTsFileGroup("one.tsfile");
    ImportTsFileScanTool.putToQueue(source.toString());
    assertEquals(CODE_OK, ImportTsFile.asyncImportTsFiles());
    assertArchived("one.tsfile");
    verify(sessionPool).executeNonQueryStatement(anyString());
    assertNull(ImportTsFileScanTool.pollFromQueue());
  }

  @Test
  public void testArchiveConflictsReturnErrorAndContinueImport() throws Exception {
    for (ImportTsFileOperation operation :
        List.of(
            ImportTsFileOperation.CP, ImportTsFileOperation.HARDLINK, ImportTsFileOperation.MV)) {
      ImportTsFileBase.setSuccessAndFailDirAndOperation(
          archive.toString(), operation, archive.toString(), operation);
      for (String conflict : List.of("", ".resource", ".mods")) {
        String name = operation + conflict + ".tsfile";
        Path source = createTsFileGroup(name);
        Path occupied = archive.resolve(name + conflict);
        Files.writeString(occupied, "existing");
        String goodName = "good-" + name;
        Path good = createTsFileGroup(goodName);
        ImportTsFileScanTool.putToQueue(source.toString());
        ImportTsFileScanTool.putToQueue(good.toString());

        assertEquals(CODE_ERROR, ImportTsFile.asyncImportTsFiles());
        assertEquals("existing", Files.readString(occupied));
        for (String suffix : List.of("", ".resource", ".mods")) {
          assertEquals("source" + suffix, Files.readString(Path.of(source + suffix)));
          if (!suffix.equals(conflict)) {
            assertFalse(Files.exists(archive.resolve(name + suffix)));
          }
        }
        assertArchived(goodName);
        assertNull(ImportTsFileScanTool.pollFromQueue());
      }
    }
    verify(sessionPool, times(18)).executeNonQueryStatement(anyString());
  }

  @Test
  public void testArchiveIoFailureReturnsError() throws Exception {
    Files.createDirectories(input.resolve("nested"));
    Path source = createTsFileGroup("nested/one.tsfile");
    Files.writeString(archive.resolve("nested"), "blocking file");
    ImportTsFileScanTool.putToQueue(source.toString());
    assertEquals(CODE_ERROR, ImportTsFile.asyncImportTsFiles());
    assertEquals("source", Files.readString(source));
    assertEquals("blocking file", Files.readString(archive.resolve("nested")));
  }

  @Test
  public void testLoadFailureReturnsErrorWhenArchiveSucceeds() throws Exception {
    Path source = createTsFileGroup("one.tsfile");
    doThrow(new StatementExecutionException("load failed"))
        .when(sessionPool)
        .executeNonQueryStatement(anyString());
    ImportTsFileScanTool.putToQueue(source.toString());
    assertEquals(CODE_ERROR, ImportTsFile.asyncImportTsFiles());
    assertArchived("one.tsfile");
    verify(sessionPool).executeNonQueryStatement(anyString());
  }

  @Test
  public void testExitCodeResetsForNextImport() throws Exception {
    Path source = createTsFileGroup("one.tsfile");
    Path occupied = archive.resolve("one.tsfile");
    Files.writeString(occupied, "existing");
    ImportTsFileScanTool.putToQueue(source.toString());
    assertEquals(CODE_ERROR, ImportTsFile.asyncImportTsFiles());
    Files.delete(occupied);
    ImportTsFileScanTool.putToQueue(source.toString());
    assertEquals(CODE_OK, ImportTsFile.asyncImportTsFiles());
    assertArchived("one.tsfile");
    verify(sessionPool, times(2)).executeNonQueryStatement(anyString());
  }

  @Test
  public void testImportFromTargetPathReturnsErrorForUnprocessedFiles() throws Exception {
    setImportOption("threadNum", 0);
    // Exercise the entry point without starting workers or connecting to a server.
    setImportOption("isRemoteLoad", true);
    Path source = createTsFileGroup("one.tsfile");
    setImportOption("source", source.toString());
    assertEquals(CODE_ERROR, ImportTsFile.importFromTargetPath());
    assertEquals(source.toString(), ImportTsFileScanTool.pollFromQueue());
  }

  private Path createTsFileGroup(String name) throws Exception {
    Path source = input.resolve(name);
    for (String suffix : List.of("", ".resource", ".mods")) {
      Files.writeString(Path.of(source + suffix), "source" + suffix);
    }
    return source;
  }

  private void assertArchived(String name) throws Exception {
    for (String suffix : List.of("", ".resource", ".mods")) {
      assertEquals("source" + suffix, Files.readString(archive.resolve(name + suffix)));
    }
  }

  private static void setImportOption(String name, Object value) throws Exception {
    Field field = ImportTsFile.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(null, value);
  }
}
