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
package org.apache.iotdb.db.it.query;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.it.env.EnvFactory;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBLoadPlainTsFileIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEncryptFlag(true)
        .setEncryptType("org.apache.tsfile.encrypt.AES128");
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void loadNormalTsFileTest() {
    String[] retArray =
        new String[] {
          "2,1,", "3,1,", "4,1,",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.tesgsg");
      statement.execute("CREATE TIMESERIES root.testsg.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      File tsfile = generateNormalFile();
      statement.execute(String.format("load \"%s\"", tsfile.getParentFile().getAbsolutePath()));
      ResultSet resultSet = statement.executeQuery("select s1 from root.testsg.d1");
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      List<Integer> actualIndexToExpectedIndexList =
          checkHeader(
              resultSetMetaData,
              "Time,root.testsg.d1.s1,",
              new int[] {
                Types.TIMESTAMP, Types.INTEGER,
              });

      int cnt = 0;
      while (resultSet.next()) {
        String[] expectedStrings = retArray[cnt].split(",");
        StringBuilder expectedBuilder = new StringBuilder();
        StringBuilder actualBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          actualBuilder.append(resultSet.getString(i)).append(",");
          expectedBuilder
              .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
              .append(",");
        }
        Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
        cnt++;
      }
      Assert.assertEquals(3, cnt);
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("The encryption way of the TsFile is not supported."));
    }
  }

  private File generateNormalFile() throws IOException {
    Path tempDir = Files.createTempDirectory("");
    tempDir.toFile().deleteOnExit();
    String tsfileName =
        TsFileNameGenerator.generateNewTsFileName(System.currentTimeMillis(), 1, 0, 0);
    File tsfile = new File(tempDir + File.separator + tsfileName);
    Files.createFile(tsfile.toPath());

    try (TsFileIOWriter writer = new TsFileIOWriter(tsfile)) {
      writer.startChunkGroup(IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d1"));
      ChunkWriterImpl chunkWriter =
          new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT32));
      chunkWriter.write(2, 1);
      chunkWriter.write(3, 1);
      chunkWriter.write(4, 1);
      chunkWriter.sealCurrentPage();

      chunkWriter.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.endFile();
    }
    return tsfile;
  }

  private List<Integer> checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeaderStrings.split(",");
    Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
    for (int i = 0; i < expectedHeaders.length; ++i) {
      expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
    }

    List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
      Assert.assertNotNull(typeIndex);
      Assert.assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
      actualIndexToExpectedIndexList.add(typeIndex);
    }
    return actualIndexToExpectedIndexList;
  }
}
