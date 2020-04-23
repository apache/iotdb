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

package org.apache.iotdb.cluster.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class TestUtils {

  private TestUtils() {
    // util class
  }

  public static Node getNode(int nodeNum) {
    Node node = new Node();
    node.setIp("192.168.0." + nodeNum);
    node.setMetaPort(ClusterDescriptor.getInstance().getConfig().getLocalMetaPort());
    node.setDataPort(ClusterDescriptor.getInstance().getConfig().getLocalDataPort());
    node.setNodeIdentifier(nodeNum);
    return node;
  }

  public static List<Log> prepareNodeLogs(int logNum) {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < logNum; i++) {
      AddNodeLog log = new AddNodeLog();
      log.setNewNode(getNode(i));
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      log.setPreviousLogIndex(i - 1L);
      log.setPreviousLogTerm(i - 1L);
      logList.add(log);
    }
    return logList;
  }

  public static StartUpStatus getStartUpStatus() {
    StartUpStatus startUpStatus = new StartUpStatus();
    startUpStatus
        .setPartitionInterval(IoTDBDescriptor.getInstance().getConfig().getPartitionInterval());
    startUpStatus.setHashSalt(ClusterConstant.HASH_SALT);
    startUpStatus
        .setReplicationNumber(ClusterDescriptor.getInstance().getConfig().getReplicationNum());
    return startUpStatus;
  }

  public static List<Log> prepareTestLogs(int logNum) {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < logNum; i++) {
      Log log = new TestLog();
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      log.setPreviousLogIndex(i - 1L);
      log.setPreviousLogTerm(i - 1L);
      logList.add(log);
    }
    return logList;
  }

  public static List<TimeValuePair> getTestTimeValuePairs(int offset, int size, int step,
      TSDataType dataType) {
    List<TimeValuePair> ret = new ArrayList<>(size);
    long currTime = offset;
    for (int i = 0; i < size; i++) {
      TsPrimitiveType value = TsPrimitiveType.getByType(dataType, currTime);
      TimeValuePair pair = new TimeValuePair(currTime, value);
      currTime += step;
      ret.add(pair);
    }
    return ret;
  }

  public static List<BatchData> getTestBatches(int offset, int size, int batchSize, int step,
      TSDataType dataType) {
    List<BatchData> ret = new ArrayList<>(size);
    long currTime = offset;
    BatchData currBatch = null;
    for (int i = 0; i < size; i++) {
      if (i % batchSize == 0) {
        if (currBatch != null) {
          ret.add(currBatch);
        }
        currBatch = new BatchData(dataType);
      }
      TsPrimitiveType value = TsPrimitiveType.getByType(dataType, currTime);
      currBatch.putAnObject(currTime, value.getValue());
      currTime += step;
    }
    if (currBatch != null) {
      ret.add(currBatch);
    }
    return ret;
  }

  public static PartitionTable getPartitionTable(int nodeNum) {
    List<Node> nodes = new ArrayList<>();
    for (int i = 0; i < nodeNum; i++) {
      nodes.add(getNode(i));
    }
    return new SlotPartitionTable(nodes, getNode(0));
  }

  public static String getTestSg(int i) {
    return "root.test" + i;
  }

  public static String getTestSeries(int sgNum, int seriesNum) {
    return getTestSg(sgNum) + "." + getTestMeasurement(seriesNum);
  }

  public static String getTestMeasurement(int seriesNum) {
    return "s" + seriesNum;
  }

  public static MeasurementSchema getTestSchema(int sgNum, int seriesNum) {
    String path = getTestSeries(sgNum, seriesNum);
    TSDataType dataType = TSDataType.DOUBLE;
    TSEncoding encoding = IoTDBDescriptor.getInstance().getConfig().getDefaultDoubleEncoding();
    return new MeasurementSchema(path, dataType, encoding, CompressionType.UNCOMPRESSED,
        Collections.emptyMap());
  }

  public static BatchData genBatchData(TSDataType dataType, int offset, int size) {
    BatchData batchData = new BatchData(dataType);
    for (long i = offset; i < offset + size; i++) {
      switch (dataType) {
        case DOUBLE:
          batchData.putDouble(i, i * 1.0);
          break;
        case TEXT:
          batchData.putBinary(i, new Binary(String.valueOf(i)));
          break;
        case INT64:
          batchData.putLong(i, i);
          break;
        case INT32:
          batchData.putInt(i, (int) i);
          break;
        case FLOAT:
          batchData.putFloat(i, i * 1.0f);
          break;
        case BOOLEAN:
          batchData.putBoolean(i, (i % 2) == 1);
          break;
      }
    }
    return batchData;
  }

  public static boolean batchEquals(BatchData batchA, BatchData batchB) {
    if (batchA == batchB) {
      return true;
    }
    if (batchA == null || batchB == null) {
      return false;
    }
    if (!batchA.getDataType().equals(batchB.getDataType())) {
      return false;
    }
    if (batchA.length() != batchB.length()) {
      return false;
    }
    while (batchA.hasCurrent()) {
      if (!batchB.hasCurrent()) {
        return false;
      }
      long timeA = batchA.currentTime();
      Object valueA = batchA.currentValue();
      long timeB = batchB.currentTime();
      Object valueB = batchB.currentValue();
      if (timeA != timeB || !valueA.equals(valueB)) {
        return false;
      }
      batchA.next();
      batchB.next();
    }
    return true;
  }

  public static void prepareData()
      throws QueryProcessException {
    InsertPlan insertPlan = new InsertPlan();
    // data for raw data query and aggregation
    // 10 devices (storage groups)
    for (int j = 0; j < 10; j++) {
      insertPlan.setDeviceId(getTestSg(j));
      String[] measurements = new String[10];
      MeasurementSchema[] schemas = new MeasurementSchema[10];
      // 10 series each device, all double
      for (int i = 0; i < 10; i++) {
        measurements[i] = getTestMeasurement(i);
        schemas[i] = TestUtils.getTestSchema(j, i);
      }
      insertPlan.setMeasurements(measurements);
      insertPlan.setSchemas(schemas);
      // the first sequential file
      for (int i = 10; i < 20; i++) {
        insertPlan.setTime(i);
        String[] values = new String[10];
        for (int k = 0; k < 10; k++) {
          values[k] = String.valueOf(i);
        }
        insertPlan.setValues(values);
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      StorageEngine.getInstance().syncCloseAllProcessor();
      // the first unsequential file, not overlapped with the sequential file
      for (int i = 0; i < 10; i++) {
        insertPlan.setTime(i);
        String[] values = new String[10];
        for (int k = 0; k < 10; k++) {
          values[k] = String.valueOf(i);
        }
        insertPlan.setValues(values);
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      StorageEngine.getInstance().syncCloseAllProcessor();
      // the second unsequential file, overlapped with the sequential file
      for (int i = 10; i < 20; i++) {
        insertPlan.setTime(i);
        String[] values = new String[10];
        for (int k = 0; k < 10; k++) {
          values[k] = String.valueOf(i);
        }
        insertPlan.setValues(values);
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      StorageEngine.getInstance().syncCloseAllProcessor();
    }

    // data for fill
    insertPlan.setDeviceId(getTestSg(0));
    String[] measurements = new String[]{getTestMeasurement(10)};
    MeasurementSchema[] schemas = new MeasurementSchema[]{TestUtils.getTestSchema(0, 10)};
    insertPlan.setMeasurements(measurements);
    insertPlan.setSchemas(schemas);
    for (int i : new int[]{0, 10}) {
      insertPlan.setTime(i);
      String[] values = new String[]{String.valueOf(i)};
      insertPlan.setValues(values);
      PlanExecutor planExecutor = new PlanExecutor();
      planExecutor.processNonQuery(insertPlan);
    }
  }
}
