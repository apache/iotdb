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

package org.apache.iotdb.cli.fs;

import org.apache.iotdb.cli.fs.provider.FilesystemMutationProvider;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class FilesystemPipelineFollowTest {

  @Rule public TemporaryFolder temporary = new TemporaryFolder();

  private FilesystemSchemaProvider provider;
  private FilesystemMutationProvider mutations;
  private ByteArrayOutputStream out;
  private ByteArrayOutputStream err;
  private CliContext context;

  @Before
  public void setUp() {
    provider = mock(FilesystemSchemaProvider.class);
    mutations = mock(FilesystemMutationProvider.class);
    out = new ByteArrayOutputStream();
    err = new ByteArrayOutputStream();
    context = context("one\ntwo\n");
  }

  @Test(timeout = 2000)
  public void followingFirstPipelineStageIsRejectedBeforeReading() {
    reject("tail -f /db/a.csv | wc -c");
  }

  @Test(timeout = 2000)
  public void followingLaterStageIsRejectedBeforeEarlierMutation() {
    reject("tee /db/a.csv | tail -f /db/b.csv");
  }

  @Test(timeout = 2000)
  public void redirectedFollowDoesNotTruncateExistingOutput() throws Exception {
    Path target = temporary.newFile("existing.txt").toPath();
    Files.write(target, "preserved".getBytes(StandardCharsets.UTF_8));

    reject("tail -f /db/a.csv > '" + target + "'");

    assertEquals("preserved", new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
  }

  @Test(timeout = 2000)
  public void redirectedFollowDoesNotCreateOutput() {
    Path target = temporary.getRoot().toPath().resolve("absent.txt");

    reject("tail -f /db/a.csv >> '" + target + "'");

    assertFalse(Files.exists(target));
  }

  @Test(timeout = 2000)
  public void followedInputRedirectIsRejectedBeforeOpeningMissingInput() {
    Path source = temporary.getRoot().toPath().resolve("missing.txt");

    reject("tail -f < '" + source + "'");

    assertTrue(err.toString().contains(CliMessages.FS_FOLLOW_COMPOUND));
  }

  @Test(timeout = 2000)
  public void offlineFollowPipelineReportsUsageWithoutLogin() {
    assertEquals(
        Integer.valueOf(FilesystemShell.USAGE_ERROR),
        FilesystemShell.runOffline(context, "tail -f /db/a.csv | wc -c", true));
    assertTrue(err.toString().contains(CliMessages.FS_FOLLOW_COMPOUND));
  }

  @Test
  public void stdinByteCountRunsOfflineWithoutAddingNewline() {
    assertEquals(
        Integer.valueOf(FilesystemShell.SUCCESS),
        FilesystemShell.runOffline(context("abc"), "wc -c"));
    assertEquals("3\n", out.toString());
  }

  @Test
  public void standaloneStdinTailFollowStillFinishesAtEof() {
    assertEquals(
        Integer.valueOf(FilesystemShell.SUCCESS),
        FilesystemShell.runOffline(context, "tail -f -n 1 -"));
    assertEquals("two\n", out.toString());
  }

  @Test
  public void remoteOperandsStillRequireConnection() {
    assertNull(FilesystemShell.runOffline(context, "wc -c /db/a.csv"));
    assertNull(FilesystemShell.runOffline(context, "tee /db/a.csv", true));
    assertNull(FilesystemShell.runOffline(context, "tee -", true));
    assertEquals("", out.toString());
  }

  private void reject(String command) {
    assertEquals(
        FilesystemShell.USAGE_ERROR,
        new FilesystemShell(context, provider, mutations, true).runNonInteractive(command));
    assertEquals("", out.toString());
    assertTrue(err.toString().contains(CliMessages.FS_FOLLOW_COMPOUND));
    verifyZeroInteractions(provider, mutations);
  }

  private CliContext context(String input) {
    return new CliContext(
        new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
        new PrintStream(out),
        new PrintStream(err),
        ExitType.EXCEPTION);
  }
}
