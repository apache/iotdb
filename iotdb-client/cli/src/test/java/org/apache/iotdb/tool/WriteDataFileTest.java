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

import org.apache.iotdb.tool.data.AbstractDataTool;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class WriteDataFileTest {

  /**
   * Create the 'target' directory before the tests run. When running tests with multiple threads,
   * the working directory might be 'target/fork_#' which would changes the assumption that the
   * 'target' directory exists already. When running tests via an IDE, the working directory might
   * be the project directory, and the target directory likely already exists.
   */
  @Before
  public void createTestDirectory() {
    new File("target").mkdirs();
  }

  @Test
  public void writeCsvFileTest() {
    List<String> headerNames =
        new ArrayList<>(Arrays.asList("Time", "column1", "column2", "column3"));

    List<Object> row1 = new ArrayList<>(Arrays.asList(1, null, "hello,world", true));
    List<Object> row2 = new ArrayList<>(Arrays.asList(2, "", "hello,world", false));
    List<Object> row3 = new ArrayList<>(Arrays.asList(3, "100", "hello world!!!", false));
    ArrayList<List<Object>> records = new ArrayList<>(Arrays.asList(row1, row2, row3));

    assertTrue(AbstractDataTool.writeCsvFile(headerNames, records, "./target/test0.csv"));
    assertTrue(AbstractDataTool.writeCsvFile(null, records, "./target/test1.csv"));
  }
}
