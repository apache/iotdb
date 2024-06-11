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

package org.apache.iotdb.db.it;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.utils.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertNotNull;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBRepairDataIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRepairData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.tesgsg");
      statement.execute("CREATE TIMESERIES root.testsg.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      File tsfile = generateUnsortedFile();
      statement.execute(String.format("load \"%s\"", tsfile.getParentFile().getAbsolutePath()));

      Assert.assertFalse(validate(statement));
      statement.execute("START REPAIR DATA");

      int waitTimes = 20;
      for (int i = 0; i < waitTimes; i++) {
        boolean sorted = validate(statement);
        if (sorted) {
          return;
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      }
      Assert.fail();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  private File generateUnsortedFile() throws IOException {
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
      chunkWriter.write(5, 1);
      chunkWriter.write(4, 1);
      chunkWriter.sealCurrentPage();

      chunkWriter.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.endFile();
    }
    return tsfile;
  }

  private boolean validate(Statement statement) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.testsg.d1")) {
      assertNotNull(resultSet);
      long time = Long.MIN_VALUE;
      while (resultSet.next()) {
        long currentTime = Long.parseLong(resultSet.getString(TIMESTAMP_STR));
        if (currentTime <= time) {
          return false;
        }
        time = currentTime;
      }
    }
    return true;
  }
}
