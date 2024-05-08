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

package org.apache.iotdb.tool.unit;

import org.apache.iotdb.tool.AbstractCsvTool;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class WriteCsvFileTestUT {
  @Test
  public void writeCsvFileTest() {
    List<String> headerNames =
        new ArrayList<>(Arrays.asList("Time", "column1", "column2", "column3"));

    List<Object> row1 = new ArrayList<>(Arrays.asList(1, null, "hello,world", true));
    List<Object> row2 = new ArrayList<>(Arrays.asList(2, "", "hello,world", false));
    List<Object> row3 = new ArrayList<>(Arrays.asList(3, "100", "hello world!!!", false));
    ArrayList<List<Object>> records = new ArrayList<>(Arrays.asList(row1, row2, row3));

    assertTrue(AbstractCsvTool.writeCsvFile(headerNames, records, "./test0.csv"));
    assertTrue(AbstractCsvTool.writeCsvFile(null, records, "./test1.csv"));
  }
}
