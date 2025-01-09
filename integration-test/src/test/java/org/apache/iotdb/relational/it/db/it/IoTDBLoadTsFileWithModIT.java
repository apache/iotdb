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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.SealedTsFileRecoverPerformer;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoadTsFileWithModIT {
  private File tmpDir;

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load").toUri());
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    try {
      for (final File file : Objects.requireNonNull(tmpDir.listFiles())) {
        Files.delete(file.toPath());
      }
      Files.delete(tmpDir.toPath());
    } finally {
      EnvFactory.getEnv().cleanClusterEnvironment();
    }
  }

  private void generateFile() throws IOException, WriteProcessException, DataRegionException {
    File tsfile = new File(tmpDir, "1-1-0-0.tsfile");
    TableSchema tableSchema =
        new TableSchema(
            "t1",
            Arrays.asList(new MeasurementSchema("s1", TSDataType.BOOLEAN)),
            Arrays.asList(Tablet.ColumnCategory.FIELD));
    // generate tsfile
    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(tsfile).tableSchema(tableSchema).build()) {
      Tablet tablet =
          new Tablet(
              Collections.singletonList("s1"), Collections.singletonList(TSDataType.BOOLEAN));
      for (int i = 0; i < 5; i++) {
        tablet.addTimestamp(i, i);
        tablet.addValue(i, 0, true);
      }
      writer.write(tablet);
    }
    // generate resource file
    TsFileResource resource = new TsFileResource(tsfile);
    try (SealedTsFileRecoverPerformer performer = new SealedTsFileRecoverPerformer(resource)) {
      performer.recover();
    }
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.deserialize();
    // write mods file
    resource
        .getExclusiveModFile()
        .write(new TableDeletionEntry(new DeletionPredicate("t1"), new TimeRange(1, 2)));
    resource.getExclusiveModFile().close();
  }

  @Test
  public void test()
      throws IoTDBConnectionException,
          SQLException,
          IOException,
          DataRegionException,
          WriteProcessException {
    generateFile();
    try (final Connection connection = EnvFactory.getEnv().getTableConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(
          String.format("load \'%s\' with ('database-name'='db1')", tmpDir.getAbsolutePath()));

      statement.execute("use db1");
      try (final ResultSet resultSet = statement.executeQuery("select s1 from t1")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertTrue(resultSet.next());
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }
    }
  }
}
