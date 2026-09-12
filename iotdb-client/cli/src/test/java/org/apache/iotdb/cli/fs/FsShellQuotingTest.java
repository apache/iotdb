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

import org.apache.iotdb.cli.fs.command.FilesystemCommandParser;
import org.apache.iotdb.cli.fs.command.FsShellWords;
import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemMutationProvider;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class FsShellQuotingTest {
  private FilesystemSchemaProvider provider;
  private FilesystemMutationProvider mutations;
  private ByteArrayOutputStream output;

  @Before
  public void setUp() throws Exception {
    provider = mock(FilesystemSchemaProvider.class);
    mutations = mock(FilesystemMutationProvider.class);
    output = new ByteArrayOutputStream();
    when(provider.list(FsPath.absolute("/")))
        .thenReturn(Collections.singletonList(node("/db", FsNodeType.TABLE_DATABASE)));
    when(provider.list(FsPath.absolute("/db")))
        .thenReturn(
            Arrays.asList(
                node("/db/table1.csv", FsNodeType.TABLE_DATA_FILE),
                node("/db/table*.csv", FsNodeType.TABLE_DATA_FILE),
                node("/db/table*2.csv", FsNodeType.TABLE_DATA_FILE)));
    when(provider.describe(any(FsPath.class)))
        .thenAnswer(invocation -> node(invocation.getArgument(0).toString(), FsNodeType.UNKNOWN));
  }

  @Test
  public void escapedWildcardRemovalIsLiteral() throws Exception {
    assertEquals(0, shell("").runNonInteractive("rm /db/table\\*.csv"));
    verify(mutations).remove(FsPath.absolute("/db/table*.csv"));
    verify(mutations, never()).remove(FsPath.absolute("/db/table1.csv"));
    verify(provider, never()).list(any(FsPath.class));
  }

  @Test
  public void partiallyQuotedWildcardRemovalIsLiteral() throws Exception {
    assertEquals(0, shell("").runNonInteractive("rm /db/table'*'.csv"));
    verify(mutations).remove(FsPath.absolute("/db/table*.csv"));
    verify(provider, never()).list(any(FsPath.class));
  }

  @Test
  public void mixedLiteralAndActiveWildcardsOnlyMatchLiteralPrefix() throws Exception {
    assertEquals(0, shell("").runNonInteractive("rm /db/table'*'*.csv"));
    verify(mutations).remove(FsPath.absolute("/db/table*.csv"));
    verify(mutations).remove(FsPath.absolute("/db/table*2.csv"));
    verify(mutations, never()).remove(FsPath.absolute("/db/table1.csv"));
  }

  @Test
  public void duplicateOperandsRetainTheirOwnQuoting() throws Exception {
    org.apache.iotdb.cli.fs.command.FilesystemCommand command =
        FilesystemCommandParser.parse("cp /db/table\\*.csv /db/table*.csv");
    assertNull(command.getPathPattern(0));
    assertEquals("/db/table*.csv", command.getPathPattern(1));
    assertEquals(
        FilesystemShell.RUNTIME_ERROR,
        shell("").runNonInteractive("cp /db/table\\*.csv '/db/table*.csv'"));
    verifyZeroInteractions(mutations);
  }

  @Test
  public void copyPreservesQuotedSourceName() throws Exception {
    assertEquals(0, shell("").runNonInteractive("cp /db/table\"*\".csv /db/copy.csv"));
    verify(mutations)
        .copy(FsPath.absolute("/db/table*.csv"), FsPath.absolute("/db/copy.csv"), false);
    verify(provider, never()).list(any(FsPath.class));
  }

  @Test
  public void quoteMetadataBelongsToOperandNotPatternOption() {
    org.apache.iotdb.cli.fs.command.FilesystemCommand command =
        FilesystemCommandParser.parse("find '/db/table*' -name /db/table*");
    assertNull(command.getPathPattern(0));
    assertEquals("/db/table*", command.getPattern());
  }

  @Test
  public void singleQuotedBasicRegexRetainsBackreferences() {
    assertEquals(0, shell("aa\nab\n").runNonInteractive("grep '^\\(a\\)\\1$' -"));
    assertEquals("aa\n", output.toString());
  }

  @Test
  public void doubleQuotedRegexRetainsNonSpecialBackslash() {
    assertEquals("\\(a\\)", FilesystemCommandParser.parse("grep \"\\(a\\)\" -").getPattern());
    assertEquals("", FsShellWords.parse("''").get(0).getValue());
    assertEquals("a b", FsShellWords.parse("a\\ b").get(0).getValue());
  }

  @Test
  public void pasteQuotedTabDelimiterRemainsAnEscapeSequence() {
    assertEquals(0, shell("a\nb\n").runNonInteractive("paste -d '\\t' - -"));
    assertEquals("a\tb\n", output.toString());
  }

  @Test
  public void textSidecarsRejectDataSelectorsBeforeReading() {
    assertTrue(shell("").runNonInteractive("cat -m value /db/table.meta") != 0);
    verifyZeroInteractions(mutations);
  }

  @Test
  public void textCatPreservesEveryInputByteInPipeline() {
    assertEquals(0, shell("a\r\nb").runNonInteractive("cat - | wc -c"));
    assertEquals("4\n", output.toString());
  }

  @Test
  public void textHeadPreservesIncompleteLastLine() {
    assertEquals(0, shell("first\nlast").runNonInteractive("head -n 1 --offset 1 -"));
    assertEquals("last", output.toString());
  }

  @Test
  public void textTailRejectsUnusedOffset() {
    assertEquals(
        FilesystemShell.USAGE_ERROR, shell("first\nlast").runNonInteractive("tail --offset 1 -"));
    assertEquals("", output.toString());
  }

  @Test
  public void grepUsageErrorsUseUnixExitStatus() {
    for (String command : Arrays.asList("grep", "grep --bad pattern", "grep 'unclosed")) {
      assertEquals(command, 2, shell("").runNonInteractive(command));
    }
    assertEquals(FilesystemShell.USAGE_ERROR, shell("").runNonInteractive("cut --bad"));
    assertEquals("", output.toString());
  }

  private FilesystemShell shell(String input) {
    return new FilesystemShell(
        new CliContext(
            new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
            new PrintStream(output),
            new PrintStream(new ByteArrayOutputStream()),
            ExitType.EXCEPTION),
        provider,
        mutations,
        true);
  }

  private static FsNode node(String path, FsNodeType type) {
    FsPath fsPath = FsPath.absolute(path);
    return new FsNode(fsPath.getFileName(), fsPath, type);
  }
}
