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
package org.apache.iotdb.tool;

import org.junit.Assert;
import org.junit.Test;

public class CsvLineSplitTest {

  @Test
  public void testSplit() {
    Assert.assertArrayEquals(
        new String[] {"", "a", "b", "c", "\\\""}, ImportCsv.splitCsvLine(",a,b,c,\\\""));
    Assert.assertArrayEquals(
        new String[] {"", "a", "b", "\\'"}, ImportCsv.splitCsvLine(",a,b,\\'"));
    Assert.assertArrayEquals(
        new String[] {"", "a\",\"a", "\"a,,\"", "'"}, ImportCsv.splitCsvLine(",a\",\"a,\"a,,\",'"));
    Assert.assertArrayEquals(
        new String[] {"True", "a=\",\"a''"}, ImportCsv.splitCsvLine("True,a=\",\"a''"));
    Assert.assertArrayEquals(
        new String[] {"True", "\"a=,,,a=z//z'a\""},
        ImportCsv.splitCsvLine("True,\"a=,,,a=z//z'a\""));
  }
}
