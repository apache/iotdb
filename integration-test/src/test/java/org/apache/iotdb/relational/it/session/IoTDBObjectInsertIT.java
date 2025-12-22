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
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.io.BaseEncoding;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.record.Tablet;
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

import static org.junit.Assert.assertNull;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBObjectInsertIT {

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
  public void insertObjectTest()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String testObject =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator
            + "object-example.pt";
    File object = new File(testObject);

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
          session.executeQueryStatement("select file from object_table where time = 1")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          Assert.assertEquals(
              BytesUtils.parseObjectByteArrayToString(BytesUtils.longToBytes(object.length())),
              iterator.getString(1));
        }
      }

      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "select READ_OBJECT(file) from object_table where time = 1")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          Binary binary = iterator.getBlob(1);
          Assert.assertArrayEquals(Files.readAllBytes(Paths.get(testObject)), binary.getValues());
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
              File objectFile =
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
                          + "1.bin");
              if (objectFile.exists() && objectFile.isFile()) {
                success = true;
              }
            }
          }
        }
      }
    }
    Assert.assertTrue(success);
  }

  @Test
  public void insertObjectSegmentsTest()
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

      try (SessionDataSet dataSet =
          session.executeQueryStatement("select file from object_table where time = 1")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          assertNull(iterator.getString(1));
        }
      }

      // insert segment with wrong offset
      try {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, 1);
        tablet.addValue(rowIndex, 0, "1");
        tablet.addValue(rowIndex, 1, "5");
        tablet.addValue(rowIndex, 2, "3");
        tablet.addValue(rowIndex, 3, 37.6F);
        tablet.addValue(rowIndex, 4, false, 512L, objectSegments.get(1));
        session.insert(tablet);
      } catch (StatementExecutionException e) {
        Assert.assertEquals(TSStatusCode.OBJECT_INSERT_ERROR.getStatusCode(), e.getStatusCode());
        Assert.assertEquals(
            String.format(
                "741: The file length %d is not equal to the offset %d",
                ((objectSegments.size() - 1) * 512), 512L),
            e.getMessage());
      } finally {
        tablet.reset();
      }

      // last segment
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, 1);
      tablet.addValue(rowIndex, 0, "1");
      tablet.addValue(rowIndex, 1, "5");
      tablet.addValue(rowIndex, 2, "3");
      tablet.addValue(rowIndex, 3, 37.6F);
      tablet.addValue(
          rowIndex,
          4,
          true,
          (objectSegments.size() - 1) * 512L,
          objectSegments.get(objectSegments.size() - 1));
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
              File objectFile =
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
                          + "1.bin");
              if (objectFile.exists() && objectFile.isFile()) {
                success = true;
              }
            }
          }
        }
      }
    }
    Assert.assertTrue(success);
  }

  protected String convertPathString(String path) {
    return BaseEncoding.base32().omitPadding().encode(path.getBytes(StandardCharsets.UTF_8));
  }
}
