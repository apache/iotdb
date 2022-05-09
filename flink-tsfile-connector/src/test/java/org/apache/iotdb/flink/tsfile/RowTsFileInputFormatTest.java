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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests for RowTsFileInputFormat. */
public class RowTsFileInputFormatTest extends RowTsFileInputFormatTestBase {

  @Test
  public void testReadData() throws IOException {
    TsFileInputFormat<Row> inputFormat = prepareInputFormat(sourceTsFilePath1);

    List<String> actual = new ArrayList<>();

    try {
      inputFormat.configure(new Configuration());
      inputFormat.openInputFormat();
      FileInputSplit[] inputSplits = inputFormat.createInputSplits(2);
      Row reuse = rowTypeInfo.createSerializer(new ExecutionConfig()).createInstance();
      for (FileInputSplit inputSplit : inputSplits) {
        try {
          inputFormat.open(inputSplit);
          assertEquals(
              config.getBatchSize(), TSFileDescriptor.getInstance().getConfig().getBatchSize());
          while (!inputFormat.reachedEnd()) {
            Row row = inputFormat.nextRecord(reuse);
            actual.add(row.toString());
          }
        } finally {
          inputFormat.close();
        }
      }
    } finally {
      inputFormat.closeInputFormat();
    }

    String[] expected = {
      "+I[1, 1.2, 20, null, 2.3, 11, 19]",
      "+I[2, null, 20, 50, 25.4, 10, 21]",
      "+I[3, 1.4, 21, null, null, null, null]",
      "+I[4, 1.2, 20, 51, null, null, null]",
      "+I[6, 7.2, 10, 11, null, null, null]",
      "+I[7, 6.2, 20, 21, null, null, null]",
      "+I[8, 9.2, 30, 31, null, null, null]"
    };
    assertArrayEquals(actual.toArray(), expected);
  }

  @Test
  public void testGetter() {
    TsFileInputFormat<Row> inputFormat = prepareInputFormat(sourceTsFilePath1);

    assertEquals(parser, inputFormat.getParser());
    assertEquals(queryExpression, inputFormat.getExpression());
    assertEquals(config, inputFormat.getConfig().get());
    assertEquals(parser.getProducedType(), inputFormat.getProducedType());
  }
}
