/**
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

package org.apache.iotdb.db.engine.modification;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.*;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class  DeletionQueryTest {

  private String processorName = "root.test";

  private static String[] measurements = new String[10];
  private String dataType = TSDataType.DOUBLE.toString();
  private String encoding = TSEncoding.PLAIN.toString();
  private EngineQueryRouter router = new EngineQueryRouter();

  static {
    for (int i = 0; i < 10; i++) {
      measurements[i] = "m" + i;
    }
  }

  @Before
  public void setup() throws MetadataErrorException,
      PathErrorException, IOException, StorageEngineException, StartupException, StorageGroupException {
    EnvironmentUtils.envSetUp();

    MManager.getInstance().setStorageLevelToMTree(processorName);
    for (int i = 0; i < 10; i++) {
      MManager.getInstance().addPathToMTree(processorName + "." + measurements[i], dataType,
          encoding);
      StorageEngine.getInstance()
          .addTimeSeries(new Path(processorName, measurements[i]), TSDataType.valueOf(dataType),
              TSEncoding.valueOf(encoding), CompressionType.valueOf(TSFileConfig.compressor),
              Collections.emptyMap());
    }
  }

  @After
  public void teardown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testDeleteInBufferWriteCache() throws
      StorageEngineException, IOException {

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }

    StorageEngine.getInstance().delete(processorName, measurements[3], 50);
    StorageEngine.getInstance().delete(processorName, measurements[4], 50);
    StorageEngine.getInstance().delete(processorName, measurements[5], 30);
    StorageEngine.getInstance().delete(processorName, measurements[5], 50);

    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path(processorName, measurements[3]));
    pathList.add(new Path(processorName, measurements[4]));
    pathList.add(new Path(processorName, measurements[5]));

    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = router.query(queryExpression, TEST_QUERY_CONTEXT);

    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    assertEquals(50, count);
  }

  @Test
  public void testDeleteInBufferWriteFile() throws StorageEngineException, IOException {
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    StorageEngine.getInstance().delete(processorName, measurements[5], 50);
    StorageEngine.getInstance().delete(processorName, measurements[4], 40);
    StorageEngine.getInstance().delete(processorName, measurements[3], 30);

    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path(processorName, measurements[3]));
    pathList.add(new Path(processorName, measurements[4]));
    pathList.add(new Path(processorName, measurements[5]));

    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = router.query(queryExpression, TEST_QUERY_CONTEXT);

    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    assertEquals(70, count);
  }

  @Test
  public void testDeleteInOverflowCache() throws StorageEngineException, IOException {
    // insert into BufferWrite
    for (int i = 101; i <= 200; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    // insert into Overflow
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }

    StorageEngine.getInstance().delete(processorName, measurements[3], 50);
    StorageEngine.getInstance().delete(processorName, measurements[4], 50);
    StorageEngine.getInstance().delete(processorName, measurements[5], 30);
    StorageEngine.getInstance().delete(processorName, measurements[5], 50);

    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path(processorName, measurements[3]));
    pathList.add(new Path(processorName, measurements[4]));
    pathList.add(new Path(processorName, measurements[5]));

    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = router.query(queryExpression, TEST_QUERY_CONTEXT);

    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      count++;
    }
    assertEquals(150, count);
  }

  @Test
  public void testDeleteInOverflowFile() throws StorageEngineException, IOException {
    // insert into BufferWrite
    for (int i = 101; i <= 200; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    // insert into Overflow
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    StorageEngine.getInstance().delete(processorName, measurements[5], 50);
    StorageEngine.getInstance().delete(processorName, measurements[4], 40);
    StorageEngine.getInstance().delete(processorName, measurements[3], 30);

    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path(processorName, measurements[3]));
    pathList.add(new Path(processorName, measurements[4]));
    pathList.add(new Path(processorName, measurements[5]));

    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = router.query(queryExpression, TEST_QUERY_CONTEXT);

    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    assertEquals(170, count);
  }

  @Test
  public void testSuccessiveDeletion()
      throws StorageEngineException, IOException, InterruptedException {
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }

    StorageEngine.getInstance().delete(processorName, measurements[3], 50);
    StorageEngine.getInstance().delete(processorName, measurements[4], 50);
    StorageEngine.getInstance().delete(processorName, measurements[5], 30);
    StorageEngine.getInstance().delete(processorName, measurements[5], 50);

    StorageEngine.getInstance().syncCloseAllProcessor();

    for (int i = 101; i <= 200; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }

    StorageEngine.getInstance().delete(processorName, measurements[3], 250);
    StorageEngine.getInstance().delete(processorName, measurements[4], 250);
    StorageEngine.getInstance().delete(processorName, measurements[5], 230);
    StorageEngine.getInstance().delete(processorName, measurements[5], 250);

    StorageEngine.getInstance().syncCloseAllProcessor();

    for (int i = 201; i <= 300; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }

    StorageEngine.getInstance().delete(processorName, measurements[3], 50);
    StorageEngine.getInstance().delete(processorName, measurements[4], 50);
    StorageEngine.getInstance().delete(processorName, measurements[5], 30);
    StorageEngine.getInstance().delete(processorName, measurements[5], 50);

    StorageEngine.getInstance().syncCloseAllProcessor();

    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path(processorName, measurements[3]));
    pathList.add(new Path(processorName, measurements[4]));
    pathList.add(new Path(processorName, measurements[5]));

    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = router.query(queryExpression, TEST_QUERY_CONTEXT);

    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    assertEquals(100, count);
  }
}
