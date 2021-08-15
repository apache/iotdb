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

package org.apache.iotdb.flink.tsfile;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertArrayEquals;

/** ITCases for RowTsFileInputFormat. */
public class RowTsFileInputFormatIT extends RowTsFileInputFormatTestBase {

  private ExecutionEnvironment env;
  private StreamExecutionEnvironment senv;

  @Before
  public void prepareEnv() {
    env = ExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    senv = StreamExecutionEnvironment.getExecutionEnvironment();
    senv.setParallelism(1);
  }

  @Test
  public void testBatchExecution() throws Exception {
    // read multiple files
    TsFileInputFormat<Row> inputFormat = prepareInputFormat(null);
    inputFormat.setFilePaths(sourceTsFilePath1, sourceTsFilePath2);
    DataSet<Row> source = env.createInput(inputFormat);
    List<String> result = source.map(Row::toString).collect();
    Collections.sort(result);
    String[] expected = {
      "1,1.2,20,null,2.3,11,19",
      "10,null,20,50,25.4,10,21",
      "11,1.4,21,null,null,null,null",
      "12,1.2,20,51,null,null,null",
      "14,7.2,10,11,null,null,null",
      "15,6.2,20,21,null,null,null",
      "16,9.2,30,31,null,null,null",
      "2,null,20,50,25.4,10,21",
      "3,1.4,21,null,null,null,null",
      "4,1.2,20,51,null,null,null",
      "6,7.2,10,11,null,null,null",
      "7,6.2,20,21,null,null,null",
      "8,9.2,30,31,null,null,null",
      "9,1.2,20,null,2.3,11,19"
    };
    assertArrayEquals(expected, result.toArray());
  }

  @Test
  public void testStreamExecution() {
    // read files in a directory
    TsFileInputFormat<Row> inputFormat = prepareInputFormat(tmpDir);
    DataStream<Row> source = senv.createInput(inputFormat);
    Iterator<String> rowStringIterator = DataStreamUtils.collect(source.map(Row::toString));
    String[] result =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(rowStringIterator, 0), false)
            .sorted()
            .toArray(String[]::new);
    String[] expected = {
      "1,1.2,20,null,2.3,11,19",
      "10,null,20,50,25.4,10,21",
      "11,1.4,21,null,null,null,null",
      "12,1.2,20,51,null,null,null",
      "14,7.2,10,11,null,null,null",
      "15,6.2,20,21,null,null,null",
      "16,9.2,30,31,null,null,null",
      "2,null,20,50,25.4,10,21",
      "3,1.4,21,null,null,null,null",
      "4,1.2,20,51,null,null,null",
      "6,7.2,10,11,null,null,null",
      "7,6.2,20,21,null,null,null",
      "8,9.2,30,31,null,null,null",
      "9,1.2,20,null,2.3,11,19"
    };
    assertArrayEquals(expected, result);
  }
}
