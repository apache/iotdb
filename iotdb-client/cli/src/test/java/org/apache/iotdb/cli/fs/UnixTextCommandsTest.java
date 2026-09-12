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

import org.apache.iotdb.cli.fs.command.FilesystemCommand;
import org.apache.iotdb.cli.fs.command.FilesystemCommandParser;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UnixTextCommandsTest {
  private final ByteArrayOutputStream output = new ByteArrayOutputStream();

  @Test
  public void grepUsesRegularExpressionsAndReportsNoMatches() throws Exception {
    assertEquals(0, execute("grep '^a.*z$' file", Arrays.asList("abz", "other", "a+z")));
    assertEquals("abz\na+z\n", text());
    output.reset();
    assertEquals(1, execute("grep missing file", Arrays.asList("a", "b")));
    assertEquals("", text());
  }

  @Test
  public void grepSupportsFixedAndExtendedPatternsAndScansAllRows() throws Exception {
    List<String> lines = new ArrayList<>(Collections.nCopies(30, "other"));
    lines.add("a+b");
    assertEquals(0, execute("grep -Fn 'a+b' file", lines));
    assertEquals("31:a+b\n", text());
    output.reset();
    execute("grep -Eiv '^(abc|def)$' file", Arrays.asList("ABC", "def", "ghi"));
    assertEquals("ghi\n", text());
    output.reset();
    execute("grep 'a+b' file", Arrays.asList("aaab", "a+b"));
    assertEquals("a+b\n", text());
  }

  @Test
  public void cutPreservesEmptyFieldsAndEmitsInputOrderWithoutDuplicates() throws Exception {
    execute("cut -d, -f2,1,2 file", Arrays.asList(",b,c", "plain", "a,,c"));
    assertEquals(",b\nplain\na,\n", text());
    output.reset();
    execute("cut -s -d, -f2- file", Arrays.asList("plain", "a,b,c"));
    assertEquals("b,c\n", text());
  }

  @Test
  public void cutSupportsByteAndCharacterRanges() throws Exception {
    execute("cut -c-1,3- file", Collections.singletonList("a\u4e2dbcd"));
    assertEquals("abcd\n", text());
    output.reset();
    execute("cut -b2-4 file", Collections.singletonList("a\u4e2db"));
    assertEquals("\u4e2d\n", text());
  }

  @Test
  public void pasteSupportsDelimitersSerialAndUnequalInputLengths() throws Exception {
    execute("paste -d, first second", Arrays.asList("a", "b"), Collections.singletonList("1"));
    assertEquals("a,1\nb,\n", text());
    output.reset();
    execute("paste -s -d, first second", Arrays.asList("a", "b"), Arrays.asList("1", "2"));
    assertEquals("a,b\n1,2\n", text());
    output.reset();
    execute("paste - -", Arrays.asList("a", "b", "c"), Arrays.asList("a", "b", "c"));
    assertEquals("a\tb\nc\t\n", text());
  }

  @Test
  public void joinSupportsDuplicateKeysUnpairedRowsAndProjection() throws Exception {
    execute(
        "join -t, -a1 -eNA -o0,1.2,2.2 first second",
        Arrays.asList("a,A1", "a,A2", "b,B"),
        Collections.singletonList("a,X"));
    assertEquals("a,A1,X\na,A2,X\nb,B,NA\n", text());
    output.reset();
    execute("join -v2 first second", Arrays.asList("a A", "c C"), Arrays.asList("a X", "b Y"));
    assertEquals("b Y\n", text());
  }

  @Test
  public void joinRejectsUnsortedInputBeforeWriting() throws Exception {
    try {
      execute("join first second", Arrays.asList("b B", "a A"), Collections.singletonList("a X"));
      fail("Unsorted join input must be rejected");
    } catch (IllegalArgumentException expected) {
      assertEquals("", text());
    }
  }

  @Test
  public void findMatchesShellNamePatterns() {
    assertTrue(UnixTextCommands.matchesName("sensors.csv", "*.csv"));
    assertTrue(UnixTextCommands.matchesName("t2.csv", "t[1-3].csv"));
    assertTrue(UnixTextCommands.matchesName("ta.csv", "t[!0-9].csv"));
    assertFalse(UnixTextCommands.matchesName("t2.meta", "t?.csv"));
  }

  @SafeVarargs
  private final int execute(String input, List<String>... lines) throws Exception {
    FilesystemCommand command = FilesystemCommandParser.parse(input);
    assertFalse(command.getErrorMessage(), command.getType() == FilesystemCommand.Type.INVALID);
    return UnixTextCommands.execute(
        command, Arrays.asList(lines), new PrintStream(output, true, "UTF-8"));
  }

  private String text() {
    return new String(output.toByteArray(), StandardCharsets.UTF_8)
        .replace(System.lineSeparator(), "\n");
  }
}
