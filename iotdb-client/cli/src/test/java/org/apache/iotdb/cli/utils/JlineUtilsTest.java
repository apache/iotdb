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

package org.apache.iotdb.cli.utils;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.DefaultParser;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JlineUtilsTest {

  @Test
  public void sqlCompleterUsesSqlKeywords() {
    List<String> values = complete(JlineUtils.createCompleter("sql"), "SEL");

    assertTrue(values.contains("SELECT"));
    assertFalse(values.contains("ls"));
  }

  @Test
  public void filesystemCompleterUsesFilesystemCommands() {
    List<String> values = complete(JlineUtils.createCompleter("filesystem"), "c");

    assertTrue(values.contains("cat"));
    assertTrue(values.contains("cd"));
    assertFalse(values.contains("CREATE"));
  }

  private static List<String> complete(Completer completer, String line) {
    ParsedLine parsedLine = new DefaultParser().parse(line, line.length());
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(null, parsedLine, candidates);
    return candidates.stream().map(Candidate::value).collect(Collectors.toList());
  }
}
