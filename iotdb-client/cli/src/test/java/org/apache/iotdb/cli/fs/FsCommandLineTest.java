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

import org.apache.iotdb.cli.i18n.CliMessages;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FsCommandLineTest {
  @Test
  public void preservesOperatorsInsideQuotesAndEscapes() {
    String quoted = "grep -F 'a|b>c<d' file";
    assertEquals(Arrays.asList(quoted), FsCommandLine.parse(quoted).commands);
    assertFalse(FsCommandLine.parse(quoted).compound());
    assertFalse(FsCommandLine.parse("grep a\\|b file").compound());
    FsCommandLine line = FsCommandLine.parse("grep 'a|b' file | cut -c1");
    assertEquals(Arrays.asList("grep 'a|b' file", "cut -c1"), line.commands);
  }

  @Test
  public void parsesInputPipelineAndAppendTarget() {
    FsCommandLine line =
        FsCommandLine.parse("grep pattern < 'input file' | cut -c1 >> \"output file\"");
    assertEquals(Arrays.asList("grep pattern", "cut -c1"), line.commands);
    assertEquals("input file", line.input);
    assertEquals("output file", line.output);
    assertTrue(line.append);
    assertTrue(line.compound());
  }

  @Test
  public void reportsMalformedTargetsAsLocalizedArgumentErrors() {
    for (String command :
        Arrays.asList(
            "pwd > 'unclosed", "pwd > \"unclosed", "pwd > file\\", "pwd > ''", "pwd > \"\"")) {
      assertInvalid(command);
    }
  }

  @Test
  public void rejectsIncompleteOrAmbiguousOperators() {
    for (String command :
        Arrays.asList(
            "| pwd",
            "pwd |",
            "pwd || cat",
            "pwd >",
            "pwd > first > second",
            "cat < first < second",
            "pwd > first | cat")) {
      assertInvalid(command);
    }
  }

  @Test
  public void leavesFollowingInputValidationToExecution() {
    assertFalse(FsCommandLine.parse("tail -f /db/t.csv").compound());
    assertEquals(
        Arrays.asList("tail -f /db/t.csv", "grep pattern"),
        FsCommandLine.parse("tail -f /db/t.csv | grep pattern").commands);
    assertTrue(FsCommandLine.parse("tail -n2 /db/t.csv | grep pattern").compound());
  }

  @Test
  public void keepsSqlBodyUntouched() {
    String sql = "sql SELECT 'a|b' FROM t WHERE value > 1";
    assertEquals(Arrays.asList(sql), FsCommandLine.parse(sql).commands);
    assertFalse(FsCommandLine.parse(sql).compound());
  }

  private static void assertInvalid(String input) {
    try {
      FsCommandLine.parse(input);
      fail(input);
    } catch (IllegalArgumentException expected) {
      assertEquals(input, CliMessages.FS_PIPE_SYNTAX, expected.getMessage());
    }
  }
}
