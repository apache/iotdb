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

package org.apache.tsfile.read.query;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

public class ResultSetTest {

  private File tsfile;

  @Before
  public void setTsfile() {
    final String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
    tsfile = new File(filePath);
    if (!tsfile.getParentFile().exists()) {
      Assert.assertTrue(tsfile.getParentFile().mkdirs());
    }
  }

  @After
  public void deleteFile() throws IOException {
    if (tsfile != null) {
      Files.deleteIfExists(tsfile.toPath());
    }
  }

  @Test
  public void test1() throws IOException, WriteProcessException {
    Tablet tablet =
        new Tablet(
            "root.sg1.d1",
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.BOOLEAN),
                new MeasurementSchema("s2", TSDataType.BOOLEAN)));
    tablet.addTimestamp(0, 1);
    tablet.addValue("s1", 0, true);
    tablet.addValue("s2", 0, false);
    tablet.addTimestamp(1, 2);
    tablet.addValue("s2", 1, true);

    try (TsFileWriter writer = new TsFileWriter(tsfile)) {
      writer.registerTimeseries("root.sg1.d1", new MeasurementSchema("s1", TSDataType.BOOLEAN));
      writer.registerTimeseries("root.sg1.d1", new MeasurementSchema("s2", TSDataType.BOOLEAN));
      writer.writeTree(tablet);
    }

    try (TsFileReader tsFileReader = new TsFileReader(tsfile)) {
      // s1 s2 s3 s4
      ResultSet resultSet =
          tsFileReader.query(
              Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4"),
              0,
              2);
      ResultSet.ResultSetMetadata resultSetMetadata = resultSet.getMetadata();
      // Time s1 s2
      Assert.assertEquals(3, resultSetMetadata.getColumnNum());
      Assert.assertEquals("Time", resultSetMetadata.getColumnName(1));
      Assert.assertEquals(TSDataType.INT64, resultSetMetadata.getColumnType(1));
      Assert.assertEquals("root.sg1.d1.s1", resultSetMetadata.getColumnName(2));
      Assert.assertEquals(TSDataType.BOOLEAN, resultSetMetadata.getColumnType(2));
      Assert.assertEquals("root.sg1.d1.s2", resultSetMetadata.getColumnName(3));
      Assert.assertEquals(TSDataType.BOOLEAN, resultSetMetadata.getColumnType(3));
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(1, resultSet.getLong(1));
      Assert.assertTrue(resultSet.getBoolean(2));
      Assert.assertFalse(resultSet.getBoolean(3));
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(2, resultSet.getLong(1));
      Assert.assertTrue(resultSet.isNull(2));
      Assert.assertTrue(resultSet.getBoolean(3));
    }
  }
}
