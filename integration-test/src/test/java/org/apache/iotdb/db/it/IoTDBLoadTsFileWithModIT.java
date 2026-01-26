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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.SealedTsFileRecoverPerformer;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
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

  private void generateFileWithNewModFile()
      throws IOException, WriteProcessException, IllegalPathException, DataRegionException {
    TsFileResource resource = generateFile();
    // write mods file
    resource
        .getExclusiveModFile()
        .write(new TreeDeletionEntry(new MeasurementPath("root.test.d1.de.s1"), 1, 2));
    resource.getExclusiveModFile().close();
  }

  private void generateFileWithOldModFile()
      throws IOException, DataRegionException, WriteProcessException, IllegalPathException {
    TsFileResource resource = generateFile();
    ModificationFileV1 oldModFile = ModificationFileV1.getNormalMods(resource);
    oldModFile.write(new Deletion(new MeasurementPath("root.test.d1.de.s1"), Long.MAX_VALUE, 1, 2));
    oldModFile.close();
  }

  private TsFileResource generateFile()
      throws WriteProcessException, IOException, DataRegionException {
    File tsfile = new File(tmpDir, "1-1-0-0.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsfile)) {
      writer.registerAlignedTimeseries(
          "root.test.d1.de",
          Collections.singletonList(new MeasurementSchema("s1", TSDataType.BOOLEAN)));
      Tablet tablet =
          new Tablet(
              "root.test.d1.de",
              Collections.singletonList(new MeasurementSchema("s1", TSDataType.BOOLEAN)));
      for (int i = 0; i < 5; i++) {
        tablet.addTimestamp(i, i);
        tablet.addValue(i, 0, true);
      }
      writer.writeTree(tablet);
    }
    // generate resource file
    TsFileResource resource = new TsFileResource(tsfile);
    try (SealedTsFileRecoverPerformer performer = new SealedTsFileRecoverPerformer(resource)) {
      performer.recover();
    }
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.deserialize();
    return resource;
  }

  @Test
  public void testWithNewModFile()
      throws SQLException,
          IOException,
          DataRegionException,
          WriteProcessException,
          IllegalPathException {
    generateFileWithNewModFile();
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(String.format("load \'%s\'", tmpDir.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery("select count(s1) as c from root.test.d1.de")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(3, resultSet.getLong("c"));
      }
    }
  }

  @Test
  public void testWithNewModFileAndLoadAttributes()
      throws SQLException,
          IOException,
          DataRegionException,
          WriteProcessException,
          IllegalPathException {
    generateFileWithNewModFile();
    final String databaseName = "root.test.d1";

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(
          String.format(
              "load \'%s\' with ("
                  + "'database-name'='%s',"
                  + "'database-level'='2',"
                  + "'verify'='true',"
                  + "'on-success'='none',"
                  + "'async'='true')",
              tmpDir.getAbsolutePath(), databaseName));

      boolean databaseFound = false;
      out:
      for (int i = 0; i < 10; i++) {
        try (final ResultSet resultSet = statement.executeQuery("show databases")) {
          while (resultSet.next()) {
            final String currentDatabase = resultSet.getString(1);
            if (databaseName.equalsIgnoreCase(currentDatabase)) {
              databaseFound = true;
              break out;
            }
          }

          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
      Assert.assertTrue(
          "The `database-level` parameter is not working; the generated database does not contain 'root.test.d1'.",
          databaseFound);
    }
  }

  @Test
  public void testWithOldModFile()
      throws SQLException,
          IOException,
          DataRegionException,
          WriteProcessException,
          IllegalPathException {
    generateFileWithOldModFile();
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(String.format("load \'%s\'", tmpDir.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery("select count(s1) as c from root.test.d1.de")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(3, resultSet.getLong("c"));
        Assert.assertTrue(
            new File(tmpDir, "1-1-0-0.tsfile" + ModificationFileV1.FILE_SUFFIX).exists());
        Assert.assertFalse(
            new File(tmpDir, "1-1-0-0.tsfile" + ModificationFile.FILE_SUFFIX).exists());
      }
    }
  }
}
