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

package org.apache.iotdb.relational.it.session;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import com.google.common.io.BaseEncoding;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNull;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBObjectDeleteIT {

  @BeforeClass
  public static void classSetUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db1");
    }
  }

  @After
  public void tearDown() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("DROP DATABASE IF EXISTS db1");
    }
  }

  @AfterClass
  public static void classTearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void dropObjectTableTest()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String testObject =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator
            + "object-example.pt";

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // insert table data by tablet
      List<String> columnNameList =
          Arrays.asList("region_id", "plant_id", "device_id", "temperature", "file");
      List<TSDataType> dataTypeList =
          Arrays.asList(
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.FLOAT,
              TSDataType.OBJECT);
      List<ColumnCategory> columnTypeList =
          new ArrayList<>(
              Arrays.asList(
                  ColumnCategory.TAG,
                  ColumnCategory.TAG,
                  ColumnCategory.TAG,
                  ColumnCategory.FIELD,
                  ColumnCategory.FIELD));
      Tablet tablet = new Tablet("object_table", columnNameList, dataTypeList, columnTypeList, 1);
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, 1);
      tablet.addValue(rowIndex, 0, "1");
      tablet.addValue(rowIndex, 1, "5");
      tablet.addValue(rowIndex, 2, "3");
      tablet.addValue(rowIndex, 3, 37.6F);
      tablet.addValue(rowIndex, 4, true, 0, Files.readAllBytes(Paths.get(testObject)));
      session.insert(tablet);
      tablet.reset();

      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "select READ_OBJECT(file) from object_table where time = 1")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          Binary binary = iterator.getBlob(1);
          Assert.assertArrayEquals(Files.readAllBytes(Paths.get(testObject)), binary.getValues());
        }
        session.executeNonQueryStatement("DROP TABLE IF EXISTS object_table");
      }
    }

    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertFalse(
                    objectFileExists("object_table", "1", "5", "3", "file", "1.bin")));
  }

  @Test
  public void dropObjectColumnTest()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String testObject =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator
            + "object-example.pt";

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // insert table data by tablet
      List<String> columnNameList =
          Arrays.asList("region_id", "plant_id", "device_id", "temperature", "file");
      List<TSDataType> dataTypeList =
          Arrays.asList(
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.FLOAT,
              TSDataType.OBJECT);
      List<ColumnCategory> columnTypeList =
          new ArrayList<>(
              Arrays.asList(
                  ColumnCategory.TAG,
                  ColumnCategory.TAG,
                  ColumnCategory.TAG,
                  ColumnCategory.FIELD,
                  ColumnCategory.FIELD));
      Tablet tablet = new Tablet("object_table", columnNameList, dataTypeList, columnTypeList, 1);
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, 1);
      tablet.addValue(rowIndex, 0, "1");
      tablet.addValue(rowIndex, 1, "5");
      tablet.addValue(rowIndex, 2, "3");
      tablet.addValue(rowIndex, 3, 37.6F);
      tablet.addValue(rowIndex, 4, true, 0, Files.readAllBytes(Paths.get(testObject)));
      session.insert(tablet);
      tablet.reset();

      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "select READ_OBJECT(file) from object_table where time = 1")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          Binary binary = iterator.getBlob(1);
          Assert.assertArrayEquals(Files.readAllBytes(Paths.get(testObject)), binary.getValues());
        }
        session.executeNonQueryStatement("ALTER TABLE object_table drop column file");
      }
    }

    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertFalse(
                    objectFileExists("object_table", "1", "5", "3", "file", "1.bin")));
  }

  @Test
  public void dropObjectTableWithMultipleRowsTest()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    byte[] objectBytes = readTestObjectBytes();

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      insertTwoRowsWithObjects(session, objectBytes);
      assertObjectRowsReadable(session, objectBytes);

      session.executeNonQueryStatement("drop table object_table");
    }

    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertFalse(objectFileExists("object_table", "1", "5", "3", "file", "1.bin"));
              Assert.assertFalse(objectFileExists("object_table", "1", "5", "3", "file", "2.bin"));
            });
  }

  @Test
  public void dropObjectDatabaseTest()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    byte[] objectBytes = readTestObjectBytes();

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      insertTwoRowsWithObjects(session, objectBytes);
      assertObjectRowsReadable(session, objectBytes);

      session.executeNonQueryStatement("drop database db1");
    }

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertFalse(objectFileExists("object_table", "1", "5", "3", "file", "1.bin"));
              Assert.assertFalse(objectFileExists("object_table", "1", "5", "3", "file", "2.bin"));
            });
  }

  @Test
  public void deleteObjectSegmentsTest()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String testObject =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator
            + "object-example.pt";
    byte[] objectBytes = Files.readAllBytes(Paths.get(testObject));
    List<byte[]> objectSegments = new ArrayList<>();
    for (int i = 0; i < objectBytes.length; i += 512) {
      objectSegments.add(Arrays.copyOfRange(objectBytes, i, Math.min(i + 512, objectBytes.length)));
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // insert table data by tablet
      List<String> columnNameList =
          Arrays.asList("region_id", "plant_id", "device_id", "temperature", "file");
      List<TSDataType> dataTypeList =
          Arrays.asList(
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.FLOAT,
              TSDataType.OBJECT);
      List<ColumnCategory> columnTypeList =
          new ArrayList<>(
              Arrays.asList(
                  ColumnCategory.TAG,
                  ColumnCategory.TAG,
                  ColumnCategory.TAG,
                  ColumnCategory.FIELD,
                  ColumnCategory.FIELD));
      Tablet tablet = new Tablet("object_table", columnNameList, dataTypeList, columnTypeList, 1);
      for (int i = 0; i < objectSegments.size() - 1; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, 1);
        tablet.addValue(rowIndex, 0, "1");
        tablet.addValue(rowIndex, 1, "5");
        tablet.addValue(rowIndex, 2, "3");
        tablet.addValue(rowIndex, 3, 37.6F);
        tablet.addValue(rowIndex, 4, false, i * 512L, objectSegments.get(i));
        session.insert(tablet);
        tablet.reset();
      }
      session.executeNonQueryStatement("DELETE FROM object_table where time = 1");

      try (SessionDataSet dataSet =
          session.executeQueryStatement("select file from object_table where time = 1")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          assertNull(iterator.getString(1));
        }
      }
    }

    // test object file path
    boolean success = false;
    for (DataNodeWrapper dataNodeWrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      String objectDirStr = dataNodeWrapper.getDataNodeObjectDir();
      File objectDir = new File(objectDirStr);
      if (objectDir.exists() && objectDir.isDirectory()) {
        File[] regionDirs = objectDir.listFiles();
        if (regionDirs != null) {
          for (File regionDir : regionDirs) {
            if (regionDir.isDirectory()) {
              File objectTmpFile =
                  new File(
                      regionDir,
                      convertPathString("object_table")
                          + File.separator
                          + convertPathString("1")
                          + File.separator
                          + convertPathString("5")
                          + File.separator
                          + convertPathString("3")
                          + File.separator
                          + convertPathString("file")
                          + File.separator
                          + "1.bin.tmp");
              if (objectTmpFile.exists() && objectTmpFile.isFile()) {
                success = true;
              }
            }
          }
        }
      }
    }
    Assert.assertFalse(success);
  }

  /**
   * After FLUSH seals OBJECT {@code .bin} files, DELETE must: (1) hide the row immediately, (2)
   * asynchronously unlink the sealed payload (including versioned {@code {time}_{version}.bin}),
   * (3) leave surviving sealed objects readable and on disk, and (4) fully clean remaining bins
   * when the last rows are deleted. Also asserts no leftover {@code .tmp}/{@code .back} siblings.
   */
  @Test
  public void deleteSealedObjectAfterFlushTest()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    byte[] objectBytes = readTestObjectBytes();

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      insertTwoRowsWithObjects(session, objectBytes);
      assertObjectRowsReadable(session, objectBytes);

      Assert.assertTrue(
          "sealed object for time=1 must exist after flush",
          objectFileExists("object_table", "1", "5", "3", "file", "1.bin"));
      Assert.assertTrue(
          "sealed object for time=2 must exist after flush",
          objectFileExists("object_table", "1", "5", "3", "file", "2.bin"));

      session.executeNonQueryStatement("DELETE FROM object_table WHERE time = 1");

      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "select time, READ_OBJECT(file) as file_content from object_table order by time")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(2L, iterator.getLong("time"));
        Assert.assertArrayEquals(objectBytes, iterator.getBlob("file_content").getValues());
        Assert.assertFalse(iterator.next());
      }
    }

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              Assert.assertFalse(
                  "deleted sealed object for time=1 should be unlinked",
                  objectFileExists("object_table", "1", "5", "3", "file", "1.bin"));
              Assert.assertFalse(
                  "deleted sealed object should leave no .tmp sibling",
                  objectFileExists("object_table", "1", "5", "3", "file", "1.bin.tmp"));
              Assert.assertFalse(
                  "deleted sealed object should leave no .back sibling",
                  objectFileExists("object_table", "1", "5", "3", "file", "1.bin.back"));
              Assert.assertTrue(
                  "surviving sealed object for time=2 must remain on disk",
                  objectFileExists("object_table", "1", "5", "3", "file", "2.bin"));
            });

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "select time, READ_OBJECT(file) as file_content from object_table order by time")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(2L, iterator.getLong("time"));
        Assert.assertArrayEquals(objectBytes, iterator.getBlob("file_content").getValues());
        Assert.assertFalse(iterator.next());
      }

      session.executeNonQueryStatement("DELETE FROM object_table WHERE time <= 2");
      try (SessionDataSet dataSet =
          session.executeQueryStatement("select count(*) as cnt from object_table")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0L, iterator.getLong("cnt"));
        Assert.assertFalse(iterator.next());
      }
    }

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              Assert.assertFalse(objectFileExists("object_table", "1", "5", "3", "file", "1.bin"));
              Assert.assertFalse(objectFileExists("object_table", "1", "5", "3", "file", "2.bin"));
              Assert.assertFalse(
                  objectFileExists("object_table", "1", "5", "3", "file", "2.bin.tmp"));
              Assert.assertFalse(
                  objectFileExists("object_table", "1", "5", "3", "file", "2.bin.back"));
            });
  }

  protected String convertPathString(String path) {
    return BaseEncoding.base32().omitPadding().encode(path.getBytes(StandardCharsets.UTF_8));
  }

  private static boolean objectBinExists(File parent, String fileName) {
    if (parent == null || !parent.isDirectory()) {
      return false;
    }
    File exact = new File(parent, fileName);
    if (exact.isFile()) {
      return true;
    }
    String stem = fileName;
    boolean tmp = stem.endsWith(".tmp");
    boolean back = stem.endsWith(".back");
    if (tmp) {
      stem = stem.substring(0, stem.length() - ".tmp".length());
    } else if (back) {
      stem = stem.substring(0, stem.length() - ".back".length());
    }
    if (stem.endsWith(".bin")) {
      stem = stem.substring(0, stem.length() - ".bin".length());
    }
    File[] children = parent.listFiles();
    if (children == null) {
      return false;
    }
    String prefix = stem + "_";
    for (File child : children) {
      if (!child.isFile()) {
        continue;
      }
      String name = child.getName();
      if (tmp) {
        if (name.equals(stem + ".bin.tmp")) {
          return true;
        }
      } else if (back) {
        if (name.equals(stem + ".bin.back")) {
          return true;
        }
      } else if (name.equals(stem + ".bin") || (name.startsWith(prefix) && name.endsWith(".bin"))) {
        return true;
      }
    }
    return false;
  }

  private byte[] readTestObjectBytes() throws IOException {
    String testObject =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator
            + "object-example.pt";
    return Files.readAllBytes(Paths.get(testObject));
  }

  private void insertTwoRowsWithObjects(ITableSession session, byte[] objectBytes)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> columnNameList =
        Arrays.asList("region_id", "plant_id", "device_id", "temperature", "file");
    List<TSDataType> dataTypeList =
        Arrays.asList(
            TSDataType.STRING,
            TSDataType.STRING,
            TSDataType.STRING,
            TSDataType.FLOAT,
            TSDataType.OBJECT);
    List<ColumnCategory> columnTypeList =
        new ArrayList<>(
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.TAG,
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD));
    Tablet tablet = new Tablet("object_table", columnNameList, dataTypeList, columnTypeList, 2);

    int rowIndex = tablet.getRowSize();
    tablet.addTimestamp(rowIndex, 1);
    tablet.addValue(rowIndex, 0, "1");
    tablet.addValue(rowIndex, 1, "5");
    tablet.addValue(rowIndex, 2, "3");
    tablet.addValue(rowIndex, 3, 37.6F);
    tablet.addValue(rowIndex, 4, true, 0, objectBytes);

    rowIndex = tablet.getRowSize();
    tablet.addTimestamp(rowIndex, 2);
    tablet.addValue(rowIndex, 0, "1");
    tablet.addValue(rowIndex, 1, "5");
    tablet.addValue(rowIndex, 2, "3");
    tablet.addValue(rowIndex, 3, 38.6F);
    tablet.addValue(rowIndex, 4, true, 0, objectBytes);

    session.insert(tablet);
    tablet.reset();
    session.executeNonQueryStatement("flush");
  }

  private void assertObjectRowsReadable(ITableSession session, byte[] objectBytes)
      throws IoTDBConnectionException, StatementExecutionException {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "select time, READ_OBJECT(file) as file_content from object_table order by time")) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      Assert.assertTrue(iterator.next());
      Assert.assertEquals(1L, iterator.getLong("time"));
      Assert.assertArrayEquals(objectBytes, iterator.getBlob("file_content").getValues());
      Assert.assertTrue(iterator.next());
      Assert.assertEquals(2L, iterator.getLong("time"));
      Assert.assertArrayEquals(objectBytes, iterator.getBlob("file_content").getValues());
      Assert.assertFalse(iterator.next());
    }
  }

  private boolean objectFileExists(
      String tableName,
      String regionId,
      String plantId,
      String deviceId,
      String measurement,
      String fileName) {
    for (DataNodeWrapper dataNodeWrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      String objectDirStr = dataNodeWrapper.getDataNodeObjectDir();
      File objectDir = new File(objectDirStr);
      if (!objectDir.exists() || !objectDir.isDirectory()) {
        continue;
      }
      File[] regionDirs = objectDir.listFiles();
      if (regionDirs == null) {
        continue;
      }
      for (File regionDir : regionDirs) {
        if (!regionDir.isDirectory()) {
          continue;
        }
        File parent =
            new File(
                regionDir,
                convertPathString(tableName)
                    + File.separator
                    + convertPathString(regionId)
                    + File.separator
                    + convertPathString(plantId)
                    + File.separator
                    + convertPathString(deviceId)
                    + File.separator
                    + convertPathString(measurement));
        if (objectBinExists(parent, fileName)) {
          return true;
        }
      }
    }
    return false;
  }
}
