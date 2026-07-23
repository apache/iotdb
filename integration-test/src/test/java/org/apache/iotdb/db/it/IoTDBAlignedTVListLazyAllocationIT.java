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

import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBAlignedTVListLazyAllocationIT {

  private static final String DEVICE = "root.aligned_lazy_allocation.d1";
  private static final int DATANODE_MAX_HEAP_SIZE_IN_MB = 256;
  private static final int INITIAL_ROW_COUNT = 40_000;
  private static final int NEW_COLUMN_COUNT = 1_200;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeJVMConfig()
        .setMaxHeapSize(DATANODE_MAX_HEAP_SIZE_IN_MB);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setEnableMemControl(true)
        .setPrimitiveArraySize(64)
        .setMemtableSizeThreshold(512L * 1024 * 1024)
        .setDatanodeMemoryProportion("6:1:1:1:1:1")
        .setWriteMemoryProportion("100:1");
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAddingManyColumnsAfterManyRowsDoesNotExhaustWriteMemory() throws Exception {
    int historicalBlockCount = (INITIAL_ROW_COUNT + ARRAY_SIZE - 1) / ARRAY_SIZE;
    long eagerAllocationCost =
        (long) historicalBlockCount
            * NEW_COLUMN_COUNT
            * AlignedTVList.valueListArrayMemCost(TSDataType.INT64);
    long lazyAllocationCost =
        (long) historicalBlockCount
                * NEW_COLUMN_COUNT
                * AlignedTVList.valueListArrayMemCostWithoutPrimitiveArray()
            + (long) NEW_COLUMN_COUNT * AlignedTVList.primitiveArrayMemCost(TSDataType.INT64);
    long dataNodeMaxHeapSize = DATANODE_MAX_HEAP_SIZE_IN_MB * 1024L * 1024L;

    Assert.assertTrue(
        "The eager implementation must exceed the entire DataNode heap in this scenario",
        eagerAllocationCost > dataNodeMaxHeapSize);
    Assert.assertTrue(
        "The lazy implementation must fit comfortably within the DataNode heap",
        lazyAllocationCost < dataNodeMaxHeapSize / 2);

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertInitialRows(session);

      List<String> measurements = new ArrayList<>(NEW_COLUMN_COUNT);
      List<TSDataType> dataTypes = new ArrayList<>(NEW_COLUMN_COUNT);
      List<Object> values = new ArrayList<>(NEW_COLUMN_COUNT);
      for (int i = 1; i <= NEW_COLUMN_COUNT; i++) {
        measurements.add("s" + i);
        dataTypes.add(TSDataType.INT64);
        values.add((long) i);
      }
      session.insertAlignedRecord(DEVICE, INITIAL_ROW_COUNT, measurements, dataTypes, values);

      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT COUNT(s0), COUNT(s" + NEW_COLUMN_COUNT + ") FROM " + DEVICE)) {
        Assert.assertTrue(dataSet.hasNext());
        List<org.apache.tsfile.read.common.Field> fields = dataSet.next().getFields();
        Assert.assertEquals(INITIAL_ROW_COUNT, fields.get(0).getLongV());
        Assert.assertEquals(1, fields.get(1).getLongV());
        Assert.assertFalse(dataSet.hasNext());
      }
    }
  }

  private static void insertInitialRows(ISession session) throws Exception {
    List<IMeasurementSchema> schemas =
        Collections.singletonList(new MeasurementSchema("s0", TSDataType.INT64));
    Tablet tablet = new Tablet(DEVICE, schemas);
    for (int i = 0; i < INITIAL_ROW_COUNT; i++) {
      int rowIndex = tablet.getRowSize();
      if (rowIndex == tablet.getMaxRowNumber()) {
        session.insertAlignedTablet(tablet);
        tablet.reset();
        rowIndex = 0;
      }
      tablet.addTimestamp(rowIndex, i);
      tablet.addValue("s0", rowIndex, (long) i);
    }
    if (tablet.getRowSize() > 0) {
      session.insertAlignedTablet(tablet);
    }
  }
}
